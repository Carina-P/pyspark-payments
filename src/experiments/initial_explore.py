from shlex import join

from pyspark.sql import SparkSession, functions as F, Window

spark = (
    SparkSession.builder
    .appName("PySpark Payments")
    .master("local[*]")
    .getOrCreate()
)

payments_df = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv("data/raw/payments.csv")
)

payments_extended_df = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv("data/raw/payments_extended.csv")
)

recipients_df = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv("data/raw/recipients.csv")
)

regions_df = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv("data/raw/region_targets.csv")
)

df1 = payments_df.filter(F.col("amount") > 10000)
df1.show()

df2 = payments_df.withColumn(
    "amount_category", 
    F.when(F.col("amount") < 5000, "small")
    .when(F.col("amount") <= 20000, "medium")
    .otherwise("large"))
df2.show()

df3 = payments_df.withColumn(
    "payment_year",
    F.year(F.col("payment_date"))
).withColumn(
    "payment_month",
    F.month(F.col("payment_date"))
).filter(
    F.col("amount") > 10000
)

df3.explain(mode="extended")
df3.show()

# Testa join operations
inner_joined_df = payments_df.join(
    recipients_df, 
    on="recipient_id",
    how="inner"
)
inner_joined_df.show()

left_joined_df = payments_df.join(
    recipients_df, 
    on="recipient_id",
    how="left"
)
left_joined_df.show()

right_joined_df = payments_df.join(
    recipients_df, 
    on="recipient_id",
    how="right"
)
right_joined_df.show()

full_joined_df = payments_df.join(
    recipients_df, 
    on="recipient_id",
    how="full"
)
full_joined_df.show()

left_semi_joined_df = payments_extended_df.join(
    recipients_df,
    on="recipient_id",
    how="left_semi"
)
left_semi_joined_df.show()

left_anti_joined_df = payments_extended_df.join(
    recipients_df,
    on="recipient_id",
    how="left_anti"
)
left_anti_joined_df.show()

# Testa agregation operations
completed_payments_df = payments_df.filter(F.col("status") == "completed")

all_regions_payments_df = completed_payments_df.join(
    recipients_df,
    on="recipient_id",
    how="right"
).join(
    regions_df,
    on="region",
    how="full"
)
all_regions_payments_df.show()

agg_region_df = (
    all_regions_payments_df.groupBy("region")
    .agg(
        F.coalesce(F.sum("amount"), F.lit(0)).alias("total_amount"),
        F.count("payment_id").alias("total_payments"),
        F.avg("amount").alias("avg_amount")
    )
)
agg_region_df.orderBy(F.desc("total_amount")).show()

# Testa Spark SQL
all_regions_payments_df.createOrReplaceTempView("all_regions_payments")
agg_region_sql_df = spark.sql("""
    SELECT
        region,
        COALESCE(SUM(amount), 0) AS total_amount,
        COUNT(payment_id) AS total_payments,
        AVG(amount) AS avg_amount
    FROM all_regions_payments
    GROUP BY region
    ORDER BY total_amount DESC, region ASC
""")
agg_region_sql_df.show()

payments_df.createOrReplaceTempView("payments")
recipients_df.createOrReplaceTempView("recipients")
regions_df.createOrReplaceTempView("regions")

all_regions_payments_sql_df = spark.sql("""
    SELECT 
        COALESCE(rg.region, r.region) AS region,
        COALESCE(SUM(p.amount), 0) AS total_amount,
        COUNT(p.payment_id) AS total_payments,
        AVG(p.amount) AS avg_amount
    FROM regions rg
    FULL JOIN recipients r 
        ON rg.region = r.region
    LEFT JOIN payments p 
        ON r.recipient_id = p.recipient_id
        AND p.status = 'completed'
    GROUP BY COALESCE(rg.region, r.region)
    ORDER BY total_amount DESC, region ASC
""")
all_regions_payments_sql_df.show()

# Testa window functions
recipients_total_amount_df = completed_payments_df.join(
    recipients_df,
    on="recipient_id",
    how="inner"
).groupBy("region", "recipient_id", "recipient_name").agg(F.sum("amount").alias("total_amount"))

window_spec = Window.partitionBy("region").orderBy(F.desc("total_amount"), F.asc("recipient_id"))

ordered_recipients_df = recipients_total_amount_df.withColumn(
    "highest_total_amount_in_region",
    F.row_number().over(window_spec)
).filter(F.col("highest_total_amount_in_region") <= 3
).select(
    "region","recipient_name", "total_amount"
)

ordered_recipients_df.show()

ordered_recipients_df.explain(mode="formatted")
