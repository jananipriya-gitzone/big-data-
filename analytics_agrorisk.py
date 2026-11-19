from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("AgroRisk_Phase1_Analytics") \
    .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# 1. Load Cleaned & Invalid Data from HDFS
cleaned_df = spark.read.csv("hdfs://localhost:9000/agrorisk/processed/cleaned_data", header=True, inferSchema=True)
invalid_df = spark.read.csv("hdfs://localhost:9000/agrorisk/processed/invalid_data", header=True, inferSchema=True)

# Register as temporary SQL tables
cleaned_df.createOrReplaceTempView("agrorisk_clean")
invalid_df.createOrReplaceTempView("agrorisk_invalid")

# Query 1: Crop-wise Production, Yield & Loss Analytics
crop_analytics = spark.sql("""
    SELECT 
        crop_name,
        COUNT(record_id) AS total_records,
        ROUND(SUM(cultivated_area_ha), 2) AS total_cultivated_area_ha,
        ROUND(SUM(harvested_area_ha), 2) AS total_harvested_area_ha,
        ROUND(SUM(production_tonnes), 2) AS total_production_tonnes,
        ROUND(AVG(actual_yield_kg_ha), 2) AS avg_yield_kg_ha,
        ROUND(AVG(yield_loss_percentage), 2) AS avg_yield_loss_pct
    FROM agrorisk_clean
    GROUP BY crop_name
    ORDER BY total_production_tonnes DESC
""")

# Query 2: District-wise Climate Risk & Loss Analytics
district_analytics = spark.sql("""
    SELECT 
        district_name,
        Agro_Climatic_Zone,
        COUNT(record_id) AS total_records,
        ROUND(AVG(rainfall_mm), 2) AS avg_rainfall_mm,
        ROUND(AVG(rainfall_deviation_percentage), 2) AS avg_rainfall_dev_pct,
        ROUND(AVG(drought_days), 1) AS avg_drought_days,
        ROUND(AVG(yield_loss_percentage), 2) AS avg_yield_loss_pct,
        SUM(CASE WHEN loss_risk_label IN ('High', 'Critical') THEN 1 ELSE 0 END) AS high_critical_risk_cases
    FROM agrorisk_clean
    GROUP BY district_name, Agro_Climatic_Zone
    ORDER BY avg_yield_loss_pct DESC
""")

# Query 3: Rescue Simulation & Resource Savings by Crop & Risk Level
rescue_analytics = spark.sql("""
    SELECT 
        crop_name,
        loss_risk_label,
        COUNT(record_id) AS case_count,
        ROUND(AVG(Simulated_Recovered_Yield_kg_ha), 2) AS avg_recovered_yield_kg_ha,
        ROUND(SUM(estimated_water_saving_liters), 2) AS total_water_saved_liters,
        ROUND(AVG(Estimated_Cost_Savings_INR_ha), 2) AS avg_cost_savings_inr_ha
    FROM agrorisk_clean
    GROUP BY crop_name, loss_risk_label
    ORDER BY crop_name, loss_risk_label
""")

# Query 4: Data Quality Statistics (Cleaned vs Invalid)
quality_stats = spark.sql("""
    SELECT 'Cleaned Valid Records' AS status, COUNT(*) AS record_count FROM agrorisk_clean
    UNION ALL
    SELECT 'Flagged Invalid Records' AS status, COUNT(*) AS record_count FROM agrorisk_invalid
""")

# Save aggregated tables back to HDFS
crop_analytics.coalesce(1).write.mode("overwrite").csv("hdfs://localhost:9000/agrorisk/analytics/crop_summary", header=True)
district_analytics.coalesce(1).write.mode("overwrite").csv("hdfs://localhost:9000/agrorisk/analytics/district_summary", header=True)
rescue_analytics.coalesce(1).write.mode("overwrite").csv("hdfs://localhost:9000/agrorisk/analytics/rescue_summary", header=True)
quality_stats.coalesce(1).write.mode("overwrite").csv("hdfs://localhost:9000/agrorisk/analytics/quality_summary", header=True)

print("\n--- TOP 5 CROPS BY PRODUCTION ---")
crop_analytics.show(5, truncate=False)

print("\n--- TOP 5 DISTRICTS BY YIELD LOSS % ---")
district_analytics.show(5, truncate=False)

print("\n--- DATA QUALITY SUMMARY ---")
quality_stats.show(truncate=False)

spark.stop()
