from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf, countDistinct, sum as _sum, date_trunc
from pyspark.sql.types import StringType, TimestampType
from datetime import datetime, timedelta, timezone

spark = SparkSession.builder.appName("CustomerOrderAnalysis").getOrCreate()


CUSTOMER_FILE = "dbfs:/FileStore/tables/task_DE_new_customers.csv"
ORDER_FILE = "dbfs:/FileStore/tables/task_DE_new_orders.xml"


def normalize_mobile(m):
    if not m or str(m).strip() == "":
        return None
    s = str(m).strip().replace(" ", "").replace("-", "")
    if s.startswith("+"):
        s = s[1:]
    s = s.lstrip("0")
    return s

normalize_mobile_udf = udf(normalize_mobile, StringType())

customers_df = (
    spark.read.option("header", True)
    .option("inferSchema", True)
    .csv(CUSTOMER_FILE)
    .withColumn("mobile_number", normalize_mobile_udf(col("mobile_number")))
)


orders_df = (
    spark.read
    .format("xml")
    .option("rowTag", "order")
    .load(ORDER_FILE)
)


orders_df = orders_df.withColumn(
    "order_date_time", col("order_date_time").cast(TimestampType())
)


repeat_customers_df = (
    orders_df.join(customers_df, "mobile_number", "left")
    .groupBy("customer_id")
    .agg(countDistinct("order_id").alias("orders_count"))
    .filter(col("orders_count") > 1)
)

# 📊 Monthly Order Trends
orders_df = orders_df.withColumn("month", date_trunc("month", col("order_date_time")))
monthly_trends_df = (
    orders_df.groupBy("month")
    .agg(countDistinct("order_id").alias("total_orders"))
    .orderBy("month")
)


regional_revenue_df = (
    orders_df.join(customers_df, "mobile_number", "left")
    .groupBy("region")
    .agg(_sum("total_amount").alias("total_revenue"))
    .orderBy(col("total_revenue").desc())
)

now = datetime.now(timezone.utc)
cutoff = now - timedelta(days=30)

recent_orders_df = orders_df.filter(col("order_date_time") >= cutoff.isoformat())

top_spenders_df = (
    recent_orders_df.join(customers_df, "mobile_number", "left")
    .groupBy("customer_id", "customer_name")
    .agg(_sum("total_amount").alias("total_spent"))
    .orderBy(col("total_spent").desc())
    .limit(10)
)

print(" Data Loaded Successfully!")
print(f"Customers: {customers_df.count()} rows")
print(f"Orders: {orders_df.count()} rows")

print("\nRepeat Customers:")
display(repeat_customers_df)

print("\nMonthly Order Trends:")
display(monthly_trends_df)

print("\nRegional Revenue:")
display(regional_revenue_df)

print("\nTop Spenders (Last 30 Days):")
display(top_spenders_df)
