from pyspark.sql import SparkSession, functions as F, Window


def  validate_columns(payments_df):
    return payments_df.withColumn(
        "valid_payment_id", F.expr("try_cast(payment_id as int)").isNotNull()
        ).withColumn(
            "valid_recipient_id", F.expr("try_cast(recipient_id as int)").isNotNull()
        ).withColumn(
            "valid_amount",
            F.coalesce(
                F.expr("try_cast(amount as decimal(12,2)) > 0"),
                F.lit(False)
            )
        ).withColumn(
            "valid_date", F.expr("try_to_timestamp(payment_date, 'yyyy-MM-dd')").isNotNull()
        )


def check_duplicates(raw_count, payments_df):
    deduplicated_df = payments_df.distinct()

    distinct_count = deduplicated_df.count()
    print(f"Removed duplicates: {raw_count - distinct_count}")

    duplicated_checked_df = deduplicated_df.withColumn(
        "id_occurrences",
        F.count("*").over(
            Window.partitionBy("payment_id")
        )
    )           
    return duplicated_checked_df


def check_recipients(recipients_df, payments_df):
    recipient_checked_df = (
        payments_df.join(
            recipients_df,
            F.col("recipient_id") == F.col("recipient"), 
            "left"
        )
        .withColumn(
            "known_recipient",
            F.col("recipient").isNotNull()
        )
        .drop("recipient ")
    )
    return recipient_checked_df


def validate_payments(payments_df):
    return payments_df.withColumn(
        "is_valid",
        F.col("valid_id") 
        & F.col("valid_amount") 
        & F.col("valid_date")
        & (F.col("id_occurrences") == 1)
        & F.col("known_recipient")
    )


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

recipients_df = (
    spark.read
    .option("header", True)
    .option("inferSchema", False)
    .csv("data/raw/recipients.csv")
)

validated_columns_df = validate_columns(raw_payments_df)

duplicate_checked_df = check_duplicates(raw_payments_df.count(), validated_columns_df)

recipients_lookup_df = recipients_df.select(F.col("recipient_id").alias("recipient")).distinct()

checked_recipients_df = check_recipients(recipients_lookup_df, duplicate_checked_df)

validated_payments_df = validate_payments(checked_recipients_df)

valid_payments = (
    validated_payments_df
    .filter(F.col("is_valid"))
    .select(
        F.col("payment_id").cast("int").alias("payment_id"),
        F.col("recipient_id").cast("int").alias("recipient_id"),
        F.col("amount").cast("decimal(12,2)").alias("amount"),
        F.to_date("payment_date", "yyyy-MM-dd").alias("payment_date"),
        F.col("status")
    )
)
valid_payments.show()

rejected_payments = validated_payments_df.filter(~F.col("is_valid"))
rejected_payments.show()

valid_payments = valid_payments.withColumn(
    "year", F.year(F.col("payment_date"))
).withColumn(
    "month", F.month(F.col("payment_date"))
)

valid_payments.write.mode("overwrite").partitionBy("year", "month").parquet("data/processed/valid_payments")

readback_df = spark.read.parquet("data/processed/valid_payments")
readback_df.show()
readback_df.printSchema()

readback_df.filter(
    (F.col("year") == 2026) &
    (F.col("month") == 10)
).explain(True)

readback_df.filter(
    F.col("amount") > 1500
).explain(True)
