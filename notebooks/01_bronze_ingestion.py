import os
from pyspark.sql import functions as F
from pyspark.sql.types import *

# 1. Define Unity Catalog Volume paths
VOLUME_BASE_DIR = "/Volumes/workspace/default/scada_3w_sample"
RAW_DATA_DIR = os.path.join(VOLUME_BASE_DIR, "3w_parquet", "sample")
BRONZE_TABLE_NAME = "workspace.default.bronze_telemetry"

# 3W parquet files use TIMESTAMP(NANOS), which Spark's parquet reader rejects (PARQUET_TYPE_ILLEGAL).
# Load them via pandas (nanos supported) and convert to Spark, carrying the source file path.
import pandas as pd
raw_df = spark.createDataFrame(
    pd.concat(
        [
            pd.read_parquet(os.path.join(RAW_DATA_DIR, f)).reset_index().assign(_source_file=os.path.join(RAW_DATA_DIR, f))
            for f in sorted(os.listdir(RAW_DATA_DIR)) if f.endswith(".parquet")
        ],
        ignore_index=True,
    )
)
# Enrich with audit/lineage metadata
bronze_df = raw_df \
    .withColumn("_ingested_at", F.current_timestamp())
print(f"Total raw telemetry records loaded: {bronze_df.count():,}")
bronze_df.printSchema()
#Write to Bronze Delta Table (Unity Catalog)
# Write to Unity Catalog as an append-only / overwrite Bronze Delta Table
bronze_df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(BRONZE_TABLE_NAME)
print(f"Bronze table successfully written to: {BRONZE_TABLE_NAME}")
# Validate Ingestion & Data Quality Sanity Check
# Query the newly created Delta table directly with SQL
display(spark.sql(f"""
    SELECT 
        _source_file,
        COUNT(*) as total_records,
        MIN(timestamp) as start_time,
        MAX(timestamp) as end_time,
        ROUND(AVG(`P-PDG`), 2) as avg_downhole_pressure_psi,
        ROUND(AVG(`T-PDG`), 2) as avg_downhole_temp_c
    FROM {BRONZE_TABLE_NAME}
    GROUP BY _source_file
"""))
# Preview sample sensor telemetry rows
display(spark.table(BRONZE_TABLE_NAME).limit(10))