# Databricks notebook source
from pyspark.sql.functions import col, trim, upper, to_date

# Read raw CSV from Bronze
df_bronze = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load("abfss://bronze@stdatapipelineadam.dfs.core.windows.net/ea16e415-8e3d-4a96-b1f1-5298b89d68c7")

print(f"Bronze row count: {df_bronze.count()}")
df_bronze.show()

# COMMAND ----------

from pyspark.sql.functions import col, trim, concat, lpad, try_to_date

# Clean and transform Bronze → Silver
df_silver = df_bronze \
    .withColumn("Branch_ID", trim(col("Branch_ID"))) \
    .withColumn("Dealer_ID", trim(col("Dealer_ID"))) \
    .withColumn("Product_Name", trim(col("Product_Name"))) \
    .withColumn("BranchName", trim(col("BranchName"))) \
    .withColumn("DealerName", trim(col("DealerName"))) \
    .withColumn("Revenue", col("Revenue").cast("long")) \
    .withColumn("Units_Sold", col("Units_Sold").cast("integer")) \
    .withColumn("Date", try_to_date(
        concat(
            col("Year").cast("string"),
            lpad(col("Month").cast("string"), 2, "0"),
            lpad(col("Day").cast("string"), 2, "0")
        ), "yyyyMMdd"
    )) \
    .drop("Day", "Month", "Year", "Date_ID") \
    .dropDuplicates()

# Check how many null dates we got
null_dates = df_silver.filter(col("Date").isNull()).count()
print(f"Silver row count: {df_silver.count()}")
print(f"Rows with invalid dates (set to NULL): {null_dates}")
df_silver.show()

# COMMAND ----------

# Write Silver layer as Delta format
df_silver.write \
    .format("delta") \
    .mode("overwrite") \
    .save("abfss://silver@stdatapipelineadam.dfs.core.windows.net/sales_silver")

print("Silver layer written successfully!")

# Verify it landed
display(spark.read.format("delta").load("abfss://silver@stdatapipelineadam.dfs.core.windows.net/sales_silver"))