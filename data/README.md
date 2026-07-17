# This folder is where you place your raw Parquet files before running the pipeline.
# This folder is listed in .gitignore — its contents will NEVER be committed to Git.
#
# Expected files:
#   yellow_tripdata_2024-01.parquet
#   yellow_tripdata_2024-02.parquet
#   yellow_tripdata_2024-03.parquet
#
# Inside the containers, this folder is mounted at: /data
