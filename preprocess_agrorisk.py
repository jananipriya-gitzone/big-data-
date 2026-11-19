from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# 1. Start Spark Session
spark = SparkSession.builder \
    .appName("AgroRisk_Phase1_Preprocessing") \
    .getOrCreate()

# Reduce verbose Spark console logs so output is easy to read
spark.sparkContext.setLogLevel("ERROR")

# 2. Load raw CSV from Hadoop HDFS
df = spark.read.csv("hdfs://localhost:9000/agrorisk/raw_data/agrorisk_main.csv", header=True, inferSchema=True)

# 3. Handle the 4,591 blank entries in Quality_Limiting_Factor
df_filled = df.fillna({"Quality_Limiting_Factor": "None"})

# 4. Convert flood_or_waterlogging_indicator (0/1) to Boolean (False/True)
df_typed = df_filled.withColumn(
    "flood_or_waterlogging_indicator",
    col("flood_or_waterlogging_indicator").cast("boolean")
)

# 5. Remove duplicate records based on record_id
df_dedup = df_typed.dropDuplicates(["record_id"])

# 6. Define Domain & Cross-Field Validation Rules
valid_condition = (
    col("district_name").isNotNull() &
    col("crop_name").isNotNull() &
    (col("harvested_area_ha") <= col("cultivated_area_ha")) &
    (col("soil_pH").between(3.5, 10.0)) &
    (col("humidity_percentage").between(0, 100)) &
    (col("production_tonnes") >= 0) &
    (col("rainfall_mm") >= 0)
)

# 7. Split into Cleaned Data and Invalid Data
cleaned_df = df_dedup.filter(valid_condition)
invalid_df = df_dedup.filter(~valid_condition)

# 8. Save both outputs back to HDFS
cleaned_df.write.mode("overwrite").csv("hdfs://localhost:9000/agrorisk/processed/cleaned_data", header=True)
invalid_df.write.mode("overwrite").csv("hdfs://localhost:9000/agrorisk/processed/invalid_data", header=True)

print("\n=============================================")
print(f"Total Input Rows        : {df.count()}")
print(f"Cleaned Valid Records   : {cleaned_df.count()}")
print(f"Flagged Invalid Records : {invalid_df.count()}")
print("=============================================\n")

spark.stop()
