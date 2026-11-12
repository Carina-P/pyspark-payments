from pyspark.sql import DataFrame, functions as F, Window

from src.pipeline.exceptions import DataQualityError


def transform(
    valid_payments_df: DataFrame,
    recipients_lookup_df: DataFrame
) -> tuple[DataFrame, DataFrame]:

    transformed_df = transform_payments(
        valid_payments_df, recipients_lookup_df
    )

    aggregated_df = aggregate_by_region(transformed_df)

    return transformed_df, aggregated_df


def transform_payments(
    valid_payments_df: DataFrame,
    recipients_lookup_df: DataFrame
) -> DataFrame:

    recipients_lookup_df = recipients_lookup_df.drop("recipient_id")

    transformed_payments = valid_payments_df.select(
            F.col("payment_id").cast("int").alias("payment_id"),
            F.col("recipient_id").cast("int").alias("recipient_id"),
            F.col("amount").cast("decimal(12,2)").alias("amount"),
            F.to_date("payment_date", "yyyy-MM-dd").alias("payment_date"),
            F.col("status")
        ).join(
            recipients_lookup_df,
            F.col("recipient_id") == F.col("recipient_id_as_int"),
            how="left"
        ).withColumn(
            "year", F.year(F.col("payment_date"))
        ).withColumn(
            "month", F.month(F.col("payment_date"))
        ).drop("recipient_id_as_int")
    
    return transformed_payments


def aggregate_by_region(
    payments_df: DataFrame
) -> DataFrame:

    aggregated_df = payments_df.filter(
        F.col("status") == "completed"
    ).groupBy("region"
    ).agg(
        F.sum("amount").alias("total_amount"),
        F.count("payment_id").alias("total_payments"),
        F.avg("amount").alias("avg_amount")
    ).orderBy(
        "region"
    )

    return aggregated_df   


def get_changed_payments(
    source_df: DataFrame,
    target_df: DataFrame,
) -> DataFrame:

    new_payments_df = source_df.join(target_df, on="payment_id", how="left_anti")
    updated_payments_df = source_df.join(target_df, on="payment_id", how="inner").filter(
        ~target_df.recipient_id.eqNullSafe(source_df.recipient_id)
        | ~target_df.amount.eqNullSafe(source_df.amount)
        | ~target_df.payment_date.eqNullSafe(source_df.payment_date)
        | ~target_df.status.eqNullSafe(source_df.status)
        | ~target_df.recipient_name.eqNullSafe(source_df.recipient_name)
        | ~target_df.region.eqNullSafe(source_df.region)
        | ~target_df.recipient_type.eqNullSafe(source_df.recipient_type)              
    ).select(source_df["*"])
    
    return new_payments_df.unionByName(updated_payments_df)
