# Databricks notebook source
# Load Gold Delta tables
fact_sales = spark.read.format("delta") \
    .load("abfss://gold@stdatapipelineadam.dfs.core.windows.net/fact_sales")

dim_branch = spark.read.format("delta") \
    .load("abfss://gold@stdatapipelineadam.dfs.core.windows.net/dim_branch")

dim_dealer = spark.read.format("delta") \
    .load("abfss://gold@stdatapipelineadam.dfs.core.windows.net/dim_dealer")

dim_product = spark.read.format("delta") \
    .load("abfss://gold@stdatapipelineadam.dfs.core.windows.net/dim_product")

dim_date = spark.read.format("delta") \
    .load("abfss://gold@stdatapipelineadam.dfs.core.windows.net/dim_date")

# Register as temp views for SQL queries
fact_sales.createOrReplaceTempView("fact_sales")
dim_branch.createOrReplaceTempView("dim_branch")
dim_dealer.createOrReplaceTempView("dim_dealer")
dim_product.createOrReplaceTempView("dim_product")

print("All Gold tables loaded successfully!")

# COMMAND ----------

revenue_by_product = spark.sql("""
    SELECT 
        p.Product_Name,
        SUM(f.Revenue) as Total_Revenue,
        SUM(f.Units_Sold) as Total_Units
    FROM fact_sales f
    JOIN dim_product p ON f.Product_ID = p.Product_ID
    GROUP BY p.Product_Name
    ORDER BY Total_Revenue DESC
    LIMIT 10
""")

display(revenue_by_product)

# COMMAND ----------

revenue_by_year = spark.sql("""
    SELECT 
        YEAR(f.Date) as Year,
        SUM(f.Revenue) as Total_Revenue,
        SUM(f.Units_Sold) as Total_Units
    FROM fact_sales f
    WHERE f.Date IS NOT NULL
    GROUP BY YEAR(f.Date)
    ORDER BY Year
""")

display(revenue_by_year)

# COMMAND ----------

top_dealers = spark.sql("""
    SELECT 
        d.DealerName,
        SUM(f.Revenue) as Total_Revenue,
        COUNT(*) as Total_Transactions
    FROM fact_sales f
    JOIN dim_dealer d ON f.Dealer_ID = d.Dealer_ID
    GROUP BY d.DealerName
    ORDER BY Total_Revenue DESC
    LIMIT 10
""")

display(top_dealers)

# COMMAND ----------

revenue_by_branch = spark.sql("""
    SELECT 
        b.BranchName,
        SUM(f.Revenue) as Total_Revenue,
        SUM(f.Units_Sold) as Total_Units
    FROM fact_sales f
    JOIN dim_branch b ON f.Branch_ID = b.Branch_ID
    GROUP BY b.BranchName
    ORDER BY Total_Revenue DESC
    LIMIT 10
""")

display(revenue_by_branch)

# COMMAND ----------

import matplotlib.pyplot as plt
import pandas as pd

# Convert to pandas and clean nulls
df_product = revenue_by_product.toPandas().dropna()
df_year = revenue_by_year.toPandas().dropna()
df_dealers = top_dealers.toPandas().dropna()
df_branch = revenue_by_branch.toPandas().dropna()

# Convert revenue to millions for readability
df_product['Revenue_M'] = df_product['Total_Revenue'] / 1_000_000
df_year['Revenue_M'] = df_year['Total_Revenue'] / 1_000_000
df_dealers['Revenue_M'] = df_dealers['Total_Revenue'] / 1_000_000
df_branch['Revenue_M'] = df_branch['Total_Revenue'] / 1_000_000

# Create 2x2 dashboard
fig, axes = plt.subplots(2, 2, figsize=(18, 12))
fig.suptitle('Sales Analytics Dashboard', fontsize=16, fontweight='bold')

# Chart 1: Revenue by Product
axes[0,0].barh(df_product['Product_Name'].astype(str), 
               df_product['Revenue_M'], color='steelblue')
axes[0,0].set_title('Top 10 Products by Revenue (Millions)')
axes[0,0].set_xlabel('Revenue (M)')

# Chart 2: Revenue by Year
axes[0,1].plot(df_year['Year'].astype(int), 
               df_year['Revenue_M'], marker='o', color='orange', linewidth=2)
axes[0,1].set_title('Revenue Trend by Year')
axes[0,1].set_xlabel('Year')
axes[0,1].set_ylabel('Revenue (M)')

# Chart 3: Top 10 Dealers
axes[1,0].barh(df_dealers['DealerName'].astype(str), 
               df_dealers['Revenue_M'], color='green')
axes[1,0].set_title('Top 10 Dealers by Revenue (Millions)')
axes[1,0].set_xlabel('Revenue (M)')

# Chart 4: Top 10 Branches
axes[1,1].barh(df_branch['BranchName'].astype(str), 
               df_branch['Revenue_M'], color='purple')
axes[1,1].set_title('Top 10 Branches by Revenue (Millions)')
axes[1,1].set_xlabel('Revenue (M)')

plt.tight_layout()
display(fig)