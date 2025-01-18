import pyspark
from pyspark.sql.functions import to_date, to_timestamp, count, sum, avg, col, when, round, max, current_date, datediff, rank, month, year, hour
from pyspark.sql.window import Window
from pyspark.sql.types import StringType, IntegerType, TimestampType, DoubleType
import os
 
spark_context = pyspark.SparkContext.getOrCreate(conf=(
        pyspark
        .SparkConf()
        .setAppName('batch-processing')
        .setMaster('local')
    ))
spark_context.setLogLevel("WARN")

spark = pyspark.sql.SparkSession(spark_context.getOrCreate())

POSTGRES_HOST = os.environ.get("POSTGRES_CONTAINER_NAME")
POSTGRES_PORT = os.environ.get("POSTGRES_PORT")
POSTGRES_DATABASE = os.environ.get("POSTGRES_DW_DB")
POSTGRES_USER = os.environ.get("POSTGRES_USER")
POSTGRES_PASSWORD = os.environ.get("POSTGRES_PASSWORD")

# JDBC URL to connect Postgres
jdbc_url = f"jdbc:postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DATABASE}"
jdbc_properties = {
    'user': POSTGRES_USER,
    'password': POSTGRES_PASSWORD,
    'driver': 'org.postgresql.Driver',
    'stringtype': 'unspecified'
}

# Read data from PostgreSQL 
try:
    df = spark.read.jdbc(
        jdbc_url,
        'public.retail',
        properties=jdbc_properties
    )
except Exception as e:
    print(f"Error reading data from PostgreSQL: {e}")
    raise

df.show()

# Data Cleaning & Transformation
## 1. Convert Data Type
print("Original data type: ")
df.printSchema()

df = df.withColumn("InvoiceDate", to_date("InvoiceDate", "M/d/yyyy H:mm")) \
       .withColumn("InvoiceTime", to_timestamp("InvoiceDate", "M/d/yyyy H:mm")) \
       .withColumn("InvoiceNo", df["InvoiceNo"].cast(IntegerType())) 

print("Data type after convert: ")
df.printSchema()

## 2. Handle Nulls
df = df.fillna(0, subset=["Quantity","UnitPrice"])

## 3. Add New Column
df = df.withColumn("Revenue", col("Quantity") * col("UnitPrice"))

# Data Analysis
## 1. Simple Aggregation
### Country Performance
revenue_per_country = df.groupBy("Country") \
    .agg(count(col("InvoiceNo")).alias("Total_Invoices"), \
         round(sum("Revenue"),2).alias("Total_Revenue")) \
    .orderBy(col("Total_Revenue").desc())

revenue_per_country.show(10)

### Top 10 Product
top_products = df.groupBy("Description") \
    .agg(sum("Quantity").alias("TotalQuantity")) \
    .orderBy(col("TotalQuantity").desc())

top_products.show(10)

## 2. Churn Prediction
### Last Purchase
last_purchase = df.groupBy("CustomerID") \
    .agg(max("InvoiceDate").alias("LastPurchaseDate"))

last_purchase.show(10)

### Churn Analysis
churn_analysis = last_purchase.withColumn(
    "DaysSinceLastPurchase", datediff(current_date(), col("LastPurchaseDate"))
).withColumn(
    "ChurnStatus", when(col("DaysSinceLastPurchase") > 90, "Churned").otherwise("Retained")
)

churn_analysis.show()

## 3. RFM Analysis
rfm_df = df.groupBy("CustomerID") \
    .agg(
        max("InvoiceDate").alias("LastPurchaseDate"),
        count("InvoiceNo").alias("Frequency"),
        round(sum("Revenue"),2).alias("Monetary")
    )

# Add column Recency
rfm_df = rfm_df.withColumn(
    "Recency", datediff(current_date(), col("LastPurchaseDate"))
)

rfm_df.show()

## 3. Monthly Sales Analysis
monthly_sales = df.groupBy(year("InvoiceDate").alias("Year"), month("InvoiceDate").alias("Month")) \
    .agg(round(sum("Revenue"),2).alias("MonthlyRevenue")) \
    .orderBy("Year", "Month")

monthly_sales.show()

# Write results to PostgreSQL
revenue_per_country.write.format("jdbc") \
    .option("url", jdbc_url) \
    .option("dbtable", "revenue_per_country")  \
    .option("user", POSTGRES_USER) \
    .option("password", POSTGRES_PASSWORD) \
    .option("driver", "org.postgresql.Driver") \
    .mode("overwrite") \
    .save()

top_products.write.format("jdbc") \
    .option("url", jdbc_url) \
    .option("dbtable", "top_products")  \
    .option("user", POSTGRES_USER) \
    .option("password", POSTGRES_PASSWORD) \
    .option("driver", "org.postgresql.Driver") \
    .mode("overwrite") \
    .save()

last_purchase.write.format("jdbc") \
    .option("url", jdbc_url) \
    .option("dbtable", "last_purchase")  \
    .option("user", POSTGRES_USER) \
    .option("password", POSTGRES_PASSWORD) \
    .option("driver", "org.postgresql.Driver") \
    .mode("overwrite") \
    .save()

churn_analysis.write.format("jdbc") \
    .option("url", jdbc_url) \
    .option("dbtable", "churn_analysis")  \
    .option("user", POSTGRES_USER) \
    .option("password", POSTGRES_PASSWORD) \
    .option("driver", "org.postgresql.Driver") \
    .mode("overwrite") \
    .save()

rfm_df.write.format("jdbc") \
    .option("url", jdbc_url) \
    .option("dbtable", "rfm_df")  \
    .option("user", POSTGRES_USER) \
    .option("password", POSTGRES_PASSWORD) \
    .option("driver", "org.postgresql.Driver") \
    .mode("overwrite") \
    .save()

monthly_sales.write.format("jdbc") \
    .option("url", jdbc_url) \
    .option("dbtable", "monthly_sales")  \
    .option("user", POSTGRES_USER) \
    .option("password", POSTGRES_PASSWORD) \
    .option("driver", "org.postgresql.Driver") \
    .mode("overwrite") \
    .save()

# Stop SparkSession
spark.stop()