# =============================================================================
# transform.py — PySpark ETL Pipeline
# NYC Taxi Big Data Pipeline
# =============================================================================
# PURPOSE:
#   1. Read raw NYC TLC Parquet files from /dataset/ (Jan–Mar 2025)
#   2. Enforce schema, clean anomalies (nulls, negatives, duplicates)
#   3. Enrich with engineered features (trip duration, year/month/day/hour)
#   4. Write the clean DataFrame to Hive table: nyc_taxi.taxi_trips_clean
#   5. Compute 4 KPI aggregations and write each to its own Hive table
#
# HOW TO RUN (from your laptop terminal):
#   docker exec nyc_spark_master \
#     /opt/spark/bin/spark-submit \
#       --master spark://spark-master:7077 \
#       --conf spark.sql.catalogImplementation=hive \
#       --conf spark.hadoop.hive.metastore.uris=thrift://hive-metastore:9083 \
#       --conf spark.sql.warehouse.dir=/opt/hive/warehouse \
#       --conf spark.driver.memory=2g \
#       --conf spark.executor.memory=2g \
#       --packages org.apache.spark:spark-hive_2.12:3.5.0 \
#       /app/transform.py
# =============================================================================

import sys
import logging
from glob import glob
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    IntegerType, LongType, DoubleType, TimestampType, StringType
)

# ---------------------------------------------------------------------------
# LOGGING
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# CONSTANTS
# ---------------------------------------------------------------------------
DATASET_PATH   = "/dataset/*.parquet"   # all months found automatically
HIVE_DATABASE  = "nyc_taxi"
WAREHOUSE_DIR  = "/opt/hive/warehouse"

# Business-rule thresholds (adjust if needed)
MIN_TRIP_DISTANCE = 0.1    # miles — remove zero/negative distances
MAX_TRIP_DISTANCE = 200.0  # miles — remove absurd outliers
MIN_FARE_AMOUNT   = 2.50   # USD — NYC minimum fare
MAX_FARE_AMOUNT   = 1000.0 # USD — remove data-entry errors
MIN_DURATION_MIN  = 1.0    # minutes
MAX_DURATION_MIN  = 300.0  # 5 hours max

# ---------------------------------------------------------------------------
# PAYMENT TYPE LABELS (NYC TLC official codes)
# ---------------------------------------------------------------------------
PAYMENT_LABELS = {
    1: "Credit Card",
    2: "Cash",
    3: "No Charge",
    4: "Dispute",
    5: "Unknown",
    6: "Voided Trip"
}


def create_spark_session() -> SparkSession:
    """
    Create and configure a SparkSession with Hive support.
    The hive-site.xml mounted at /opt/spark/conf/ provides metastore.uris
    automatically — but we also set them explicitly here as a safety net.
    """
    log.info("Initialising SparkSession …")
    spark = (
        SparkSession.builder
        .appName("NYC_Taxi_ETL_Pipeline")
        .master("spark://spark-master:7077")
        # Enable Hive catalog support
        .config("spark.sql.catalogImplementation", "hive")
        .config("spark.hadoop.hive.metastore.uris", "thrift://hive-metastore:9083")
        .config("spark.sql.warehouse.dir", WAREHOUSE_DIR)
        # Performance settings
        .config("spark.sql.shuffle.partitions", "50")        # tune for ~59 MB input
        .config("spark.sql.adaptive.enabled", "true")        # AQE for auto-optimization
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        # Parquet reading optimisations
        .config("spark.sql.parquet.filterPushdown", "true")
        .config("spark.sql.parquet.mergeSchema", "false")    # all files share one schema
        # Hive compatibility
        .config("spark.sql.hive.convertMetastoreParquet", "true")
        .enableHiveSupport()
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    log.info("SparkSession created successfully.")
    return spark


def read_raw_data(spark: SparkSession) -> "pyspark.sql.DataFrame":
    """
    Read all Parquet files matching /dataset/*.parquet.
    Uses schema inference on the first call; subsequent runs benefit from
    the merged schema being consistent across months.
    Raises SystemExit if no files are found.
    """
    log.info(f"Reading raw Parquet files from: {DATASET_PATH}")
    try:
        df = spark.read.parquet(DATASET_PATH)
        count = df.count()
        log.info(f"Raw data loaded — {count:,} rows, {len(df.columns)} columns.")
        log.info(f"Schema: {df.dtypes}")
        return df
    except Exception as exc:
        log.error(f"Failed to read Parquet data: {exc}")
        log.error("Make sure at least one .parquet file exists in /dataset/")
        sys.exit(1)


def clean_and_enrich(df) -> "pyspark.sql.DataFrame":
    """
    Full cleaning + feature-engineering pipeline:

    CLEANING steps:
      1. Rename columns to snake_case for consistency
      2. Drop rows where critical columns are NULL
      3. Filter invalid trip distances (<=0 or unrealistically large)
      4. Filter invalid fare amounts (<= 0 or unrealistically large)
      5. Drop exact duplicates

    ENRICHMENT steps:
      6. Cast timestamps correctly
      7. Compute trip_duration_min
      8. Extract trip_year, trip_month, trip_day, trip_hour
    """
    log.info("Starting data cleaning and enrichment …")

    # ---- Step 1: Standardise column names ----
    # NYC TLC Parquet files use mixed-case names — normalise to snake_case.
    rename_map = {
        "VendorID":            "vendor_id",
        "tpep_pickup_datetime": "pickup_datetime",
        "tpep_dropoff_datetime":"dropoff_datetime",
        "passenger_count":     "passenger_count",
        "trip_distance":       "trip_distance",
        "RatecodeID":          "rate_code_id",
        "PULocationID":        "pickup_location_id",
        "DOLocationID":        "dropoff_location_id",
        "payment_type":        "payment_type",
        "fare_amount":         "fare_amount",
        "tip_amount":          "tip_amount",
        "total_amount":        "total_amount",
        # keep only the columns we need; extras are ignored below
    }
    # Only rename columns that actually exist in this file
    existing = set(df.columns)
    for old_name, new_name in rename_map.items():
        if old_name in existing:
            df = df.withColumnRenamed(old_name, new_name)

    # Select only the columns we care about (guards against extra columns in future schema versions)
    keep_cols = [
        "vendor_id", "pickup_datetime", "dropoff_datetime",
        "passenger_count", "trip_distance", "rate_code_id",
        "pickup_location_id", "dropoff_location_id",
        "payment_type", "fare_amount", "tip_amount", "total_amount"
    ]
    # Keep only columns that exist after renaming
    keep_cols = [c for c in keep_cols if c in df.columns]
    df = df.select(keep_cols)

    # ---- Step 2: Drop rows with NULLs in critical columns ----
    critical_cols = ["pickup_datetime", "dropoff_datetime", "trip_distance",
                     "fare_amount", "total_amount"]
    critical_cols = [c for c in critical_cols if c in df.columns]
    before = df.count()
    df = df.dropna(subset=critical_cols)
    after_null = df.count()
    log.info(f"Null drop: removed {before - after_null:,} rows.")

    # ---- Step 3: Cast timestamps (they may arrive as strings in some exports) ----
    df = df.withColumn("pickup_datetime",  F.to_timestamp("pickup_datetime"))
    df = df.withColumn("dropoff_datetime", F.to_timestamp("dropoff_datetime"))

    # ---- Step 4: Filter invalid trip distances ----
    df = df.filter(
        (F.col("trip_distance") >= MIN_TRIP_DISTANCE) &
        (F.col("trip_distance") <= MAX_TRIP_DISTANCE)
    )
    after_dist = df.count()
    log.info(f"Distance filter: removed {after_null - after_dist:,} rows.")

    # ---- Step 5: Filter invalid fare amounts ----
    df = df.filter(
        (F.col("fare_amount") >= MIN_FARE_AMOUNT) &
        (F.col("fare_amount") <= MAX_FARE_AMOUNT)
    )
    after_fare = df.count()
    log.info(f"Fare filter: removed {after_dist - after_fare:,} rows.")

    # ---- Step 6: Compute trip_duration_min ----
    df = df.withColumn(
        "trip_duration_min",
        F.round(
            (F.unix_timestamp("dropoff_datetime") - F.unix_timestamp("pickup_datetime")) / 60.0,
            2
        )
    )
    # Filter unrealistic durations
    df = df.filter(
        (F.col("trip_duration_min") >= MIN_DURATION_MIN) &
        (F.col("trip_duration_min") <= MAX_DURATION_MIN)
    )
    after_dur = df.count()
    log.info(f"Duration filter: removed {after_fare - after_dur:,} rows.")

    # ---- Step 7: Extract time components ----
    df = (
        df
        .withColumn("trip_year",  F.year("pickup_datetime").cast(IntegerType()))
        .withColumn("trip_month", F.month("pickup_datetime").cast(IntegerType()))
        .withColumn("trip_day",   F.dayofmonth("pickup_datetime").cast(IntegerType()))
        .withColumn("trip_hour",  F.hour("pickup_datetime").cast(IntegerType()))
    )

    # ---- Step 8: Drop exact duplicates ----
    df = df.dropDuplicates()
    final_count = df.count()
    log.info(f"After deduplication: {final_count:,} rows remain.")

    # Cache here — multiple aggregations will be computed from this DataFrame
    df.cache()
    log.info("Clean DataFrame cached in memory for multi-KPI aggregation.")

    return df


def write_to_hive(df, table_name: str, mode: str = "overwrite", spark: SparkSession = None) -> None:
    """
    Write a DataFrame to a managed Hive table.
    Uses INSERT OVERWRITE semantics by default (idempotent re-runs).
    """
    full_table = f"{HIVE_DATABASE}.{table_name}"
    log.info(f"Writing to Hive table: {full_table}  (mode={mode}) …")
    try:
        if spark is not None:
            spark.sql(f"DROP TABLE IF EXISTS {full_table}")
        (
            df.write
            .format("parquet")
            .mode(mode)
            .option("compression", "snappy")
            .saveAsTable(full_table)
        )
        log.info(f"✅  Successfully wrote to {full_table}")
    except Exception as exc:
        log.error(f"❌  Failed writing to {full_table}: {exc}")
        raise


def compute_trips_per_hour(df) -> "pyspark.sql.DataFrame":
    """KPI 1: Trip count grouped by year, month, and hour of day."""
    return (
        df.groupBy("trip_year", "trip_month", "trip_hour")
        .agg(F.count("*").alias("total_trips"))
        .orderBy("trip_year", "trip_month", "trip_hour")
    )


def compute_revenue_summary(df) -> "pyspark.sql.DataFrame":
    """KPI 2: Daily revenue, avg fare, avg duration."""
    return (
        df.groupBy("trip_year", "trip_month", "trip_day")
        .agg(
            F.round(F.sum("total_amount"), 2).alias("total_revenue"),
            F.round(F.avg("fare_amount"), 2).alias("avg_fare"),
            F.round(F.avg("trip_duration_min"), 2).alias("avg_duration_min"),
            F.count("*").alias("total_trips")
        )
        .orderBy("trip_year", "trip_month", "trip_day")
    )


def compute_top_pickup_zones(df) -> "pyspark.sql.DataFrame":
    """KPI 3: Top pickup zones by total trips."""
    return (
        df.groupBy("pickup_location_id")
        .agg(F.count("*").alias("total_trips"))
        .orderBy(F.col("total_trips").desc())
        .limit(50)  # top 50 zones is plenty for the dashboard
    )


def compute_payment_summary(df, spark: SparkSession) -> "pyspark.sql.DataFrame":
    """KPI 4: Trip and revenue split by payment method."""
    # Build a lookup DataFrame for payment_type → human-readable label
    label_rows = [(k, v) for k, v in PAYMENT_LABELS.items()]
    labels_df = spark.createDataFrame(label_rows, ["payment_type", "payment_label"])

    result = (
        df.groupBy("payment_type")
        .agg(
            F.count("*").alias("total_trips"),
            F.round(F.sum("total_amount"), 2).alias("total_revenue"),
            F.round(F.avg("tip_amount"), 2).alias("avg_tip_amount")
        )
        .join(labels_df, on="payment_type", how="left")
        .fillna({"payment_label": "Unknown"})
        .orderBy(F.col("total_trips").desc())
    )
    return result


# =============================================================================
# MAIN
# =============================================================================
def main():
    spark = create_spark_session()

    # ---- Ensure Hive database exists ----
    log.info(f"Creating Hive database if not exists: {HIVE_DATABASE}")
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {HIVE_DATABASE} "
              f"LOCATION '{WAREHOUSE_DIR}/{HIVE_DATABASE}.db'")
    spark.sql(f"USE {HIVE_DATABASE}")

    # ---- ETL: Read & Clean ----
    raw_df   = read_raw_data(spark)
    clean_df = clean_and_enrich(raw_df)

    # ---- Write cleaned data to Hive ----
    write_to_hive(clean_df, "taxi_trips_clean", spark=spark)

    # ---- KPI 1: Trips per hour ----
    trips_hour_df = compute_trips_per_hour(clean_df)
    write_to_hive(trips_hour_df, "trips_per_hour", spark=spark)

    # ---- KPI 2: Revenue summary ----
    revenue_df = compute_revenue_summary(clean_df)
    write_to_hive(revenue_df, "revenue_summary", spark=spark)

    # ---- KPI 3: Top pickup zones ----
    zones_df = compute_top_pickup_zones(clean_df)
    write_to_hive(zones_df, "top_pickup_zones", spark=spark)

    # ---- KPI 4: Payment summary ----
    payment_df = compute_payment_summary(clean_df, spark)
    write_to_hive(payment_df, "payment_summary", spark=spark)

    # ---- Release cache ----
    clean_df.unpersist()

    log.info("=" * 60)
    log.info("🎉  NYC Taxi ETL Pipeline completed successfully!")
    log.info(f"    Tables written to Hive database: {HIVE_DATABASE}")
    log.info("    • taxi_trips_clean")
    log.info("    • trips_per_hour")
    log.info("    • revenue_summary")
    log.info("    • top_pickup_zones")
    log.info("    • payment_summary")
    log.info("=" * 60)

    spark.stop()


if __name__ == "__main__":
    main()
