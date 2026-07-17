-- =============================================================================
-- create_tables.hql — Hive DDL Setup Script
-- NYC Taxi Big Data Pipeline
-- =============================================================================
-- Run this ONCE after the Hive server starts to create all required tables.
-- Command (from inside the nyc_hive container):
--   beeline -u "jdbc:hive2://localhost:10000" -f /database_scripts/create_tables.hql
-- =============================================================================

-- Use (or create) the target database
CREATE DATABASE IF NOT EXISTS nyc_taxi
  COMMENT 'NYC Taxi Trip Records Data Warehouse'
  LOCATION '/opt/hive/warehouse/nyc_taxi.db';

USE nyc_taxi;

-- =============================================================================
-- TABLE 1: taxi_trips_clean
-- Cleaned and enriched individual trip records written by the Spark ETL job.
-- Spark writes directly to this table using saveAsTable / insertInto.
-- =============================================================================
CREATE TABLE IF NOT EXISTS taxi_trips_clean (
    vendor_id           INT,
    pickup_datetime     TIMESTAMP,
    dropoff_datetime    TIMESTAMP,
    passenger_count     INT,
    trip_distance       DOUBLE,
    rate_code_id        INT,
    pickup_location_id  INT,
    dropoff_location_id INT,
    payment_type        INT,
    fare_amount         DOUBLE,
    tip_amount          DOUBLE,
    total_amount        DOUBLE,
    trip_duration_min   DOUBLE,   -- engineered feature: duration in minutes
    trip_year           INT,      -- extracted from pickup_datetime
    trip_month          INT,
    trip_day            INT,
    trip_hour           INT
)
STORED AS PARQUET
TBLPROPERTIES ("parquet.compression"="SNAPPY");

-- =============================================================================
-- TABLE 2: trips_per_hour
-- KPI: Number of trips grouped by year, month, and hour of day.
-- =============================================================================
CREATE TABLE IF NOT EXISTS trips_per_hour (
    trip_year   INT,
    trip_month  INT,
    trip_hour   INT,
    total_trips BIGINT
)
STORED AS PARQUET
TBLPROPERTIES ("parquet.compression"="SNAPPY");

-- =============================================================================
-- TABLE 3: revenue_summary
-- KPI: Daily revenue summary with average fare and average trip duration.
-- =============================================================================
CREATE TABLE IF NOT EXISTS revenue_summary (
    trip_year          INT,
    trip_month         INT,
    trip_day           INT,
    total_revenue      DOUBLE,
    avg_fare           DOUBLE,
    avg_duration_min   DOUBLE,
    total_trips        BIGINT
)
STORED AS PARQUET
TBLPROPERTIES ("parquet.compression"="SNAPPY");

-- =============================================================================
-- TABLE 4: top_pickup_zones
-- KPI: Most active pickup zones ranked by number of trips.
-- =============================================================================
CREATE TABLE IF NOT EXISTS top_pickup_zones (
    pickup_location_id INT,
    total_trips        BIGINT
)
STORED AS PARQUET
TBLPROPERTIES ("parquet.compression"="SNAPPY");

-- =============================================================================
-- TABLE 5: payment_summary
-- KPI: Trip and revenue split by payment method type.
-- payment_type codes: 1=Credit Card, 2=Cash, 3=No Charge, 4=Dispute
-- =============================================================================
CREATE TABLE IF NOT EXISTS payment_summary (
    payment_type        INT,
    payment_label       STRING,
    total_trips         BIGINT,
    total_revenue       DOUBLE,
    avg_tip_amount      DOUBLE
)
STORED AS PARQUET
TBLPROPERTIES ("parquet.compression"="SNAPPY");

-- Confirm all tables are created
SHOW TABLES;
