from pyspark.sql import SparkSession, functions as F, Window

spark = (
    SparkSession.builder
    .appName("PySpark Payments")
    .master("local[*]")
    .getOrCreate()
)

raw_payments_df = (
    spark.read
    .option("header", True)
    .option("inferSchema", False)
    .csv("data/raw/payments_invalid.csv")
)

raw_payments_df.printSchema()
raw_payments_df.show()

validate_payments_df = raw_payments_df.withColumn(
    "valid_id", F.expr("try_cast(payment_id as int)").isNotNull()
    ).withColumn(
    "valid_amount",
    F.coalesce(
        F.expr("try_cast(amount as decimal(12,2)) > 0"),
        F.lit(False)
    )
    ).withColumn(
    "valid_date", F.expr("try_to_timestamp(payment_date, 'yyyy-MM-dd')").isNotNull()
).withColumn(
    "is_valid", F.col("valid_id") & F.col("valid_amount") & F.col("valid_date")
)

validate_payments_df.printSchema()
validate_payments_df.show()
validate_payments_df.groupBy("is_valid").count().show()

dubletts_df = validate_payments_df.groupBy("payment_id").count().filter(F.col("count") > 1)
dubletts_df.show()

duplicate_payments_df = dubletts_df.select("payment_id").join(validate_payments_df, on="payment_id", how="inner")
duplicate_payments_df.show()

# Använd window function för att markera dubbletter
window_spec = Window.partitionBy("payment_id")

duplicate_payments_df2 = (
    validate_payments_df
    .withColumn(
        "id_count",
        F.count("*").over(window_spec)
    )
    .filter(F.col("id_count") > 1)
)
duplicate_payments_df2.show()

# Ta bort alla utom 1 identisk dublett/triplett/osv 
duplicate_payments_df3 = (
    duplicate_payments_df.select(
        "payment_id",
        "recipient_id",
        "amount",
        "payment_date",
        "status"
    ).distinct()
    .groupBy("payment_id")
    .count()
)
duplicate_payments_df3.show()

# Find payments where recipient_id is missing correspponding recipient in recipients.csv
payments_extended_df = (
    spark.read
    .option("header", True)
    .option("inferSchema", False)
    .csv("data/raw/payments_extended.csv")
)

recipients_df = (
    spark.read
    .option("header", True)
    .option("inferSchema", False)
    .csv("data/raw/recipients.csv")
)

missing_recipients_df = payments_extended_df.join(
    recipients_df,
    on="recipient_id",
    how="left_anti"
)
missing_recipients_df.show()
missing_recipients_df.explain(True)
