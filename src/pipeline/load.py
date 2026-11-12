from pyspark.sql import SparkSession
from pyspark.sql import DataFrame, functions as F

from src.pipeline.exceptions import PipelineWriteError

def load(
    df: DataFrame,
    path: str,
    write_mode:str ="overwrite",
    partition_columns: tuple[str] = ()
) -> None:

    try:
        if partition_columns:
            df.write.mode(write_mode).partitionBy(partition_columns).parquet(path)
        else:
            df.write.mode(write_mode).parquet(path)
    
    except Exception as e:
        raise PipelineWriteError(
            f"Failed to write Parquet to: {path}"
        ) from e
    
    return


def merge_payments_into_iceberg(
    spark: SparkSession,
    payments_df: DataFrame,
    table_name: str,
) -> None:

    payments_df.createOrReplaceTempView("new_payments")

    spark.sql("""
        MERGE INTO IDENTIFIER(:table_name) AS target 
            USING new_payments AS source
            ON target.payment_id = source.payment_id
        WHEN MATCHED AND (
            NOT (target.recipient_id <=> source.recipient_id)
            OR NOT (target.amount <=> source.amount)
            OR NOT (target.payment_date <=> source.payment_date)
            OR NOT (target.status <=> source.status)
            OR NOT (target.recipient_name <=> source.recipient_name)
            OR NOT (target.region <=> source.region)
            OR NOT (target.recipient_type <=> source.recipient_type)
        ) THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """,
    args={"table_name": table_name})
