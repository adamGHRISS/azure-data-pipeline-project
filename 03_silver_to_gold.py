# Databricks notebook source
# Read Silver layer
df_silver = spark.read.format("delta") \
    .load("abfss://silver@stdatapipelineadam.dfs.core.windows.net/sales_silver")

print(f"Silver row count: {df_silver.count()}")
df_silver.show(5)

# COMMAND ----------

from pyspark.sql.functions import md5, concat_ws, col

# Dimension 1: Branch
dim_branch = df_silver.select("Branch_ID", "BranchName") \
    .dropDuplicates()

# Dimension 2: Dealer
dim_dealer = df_silver.select("Dealer_ID", "DealerName") \
    .dropDuplicates()

# Dimension 3: Product
dim_product = df_silver.select("Product_Name") \
    .dropDuplicates() \
    .withColumn("Product_ID", md5(col("Product_Name")))

# Dimension 4: Date
dim_date = df_silver.select("Date") \
    .dropDuplicates() \
    .filter(col("Date").isNotNull())

print(f"dim_branch: {dim_branch.count()} rows")
print(f"dim_dealer: {dim_dealer.count()} rows")
print(f"dim_product: {dim_product.count()} rows")
print(f"dim_date: {dim_date.count()} rows")

# COMMAND ----------

from pyspark.sql.functions import md5, col

# Fact table: Sales
fact_sales = df_silver \
    .withColumn("Product_ID", md5(col("Product_Name"))) \
    .select(
        "Branch_ID",
        "Dealer_ID", 
        "Product_ID",
        "Model_ID",
        "Date",
        "Revenue",
        "Units_Sold"
    ) \
    .filter(col("Date").isNotNull())

print(f"fact_sales: {fact_sales.count()} rows")
fact_sales.show(5)

# COMMAND ----------

# Write all dimension tables and fact table to Gold layer
dim_branch.write.format("delta").mode("overwrite") \
    .save("abfss://gold@stdatapipelineadam.dfs.core.windows.net/dim_branch")

dim_dealer.write.format("delta").mode("overwrite") \
    .save("abfss://gold@stdatapipelineadam.dfs.core.windows.net/dim_dealer")

dim_product.write.format("delta").mode("overwrite") \
    .save("abfss://gold@stdatapipelineadam.dfs.core.windows.net/dim_product")

dim_date.write.format("delta").mode("overwrite") \
    .save("abfss://gold@stdatapipelineadam.dfs.core.windows.net/dim_date")

fact_sales.write.format("delta").mode("overwrite") \
    .save("abfss://gold@stdatapipelineadam.dfs.core.windows.net/fact_sales")

print("Gold layer written successfully!")
print("Tables written: dim_branch, dim_dealer, dim_product, dim_date, fact_sales")