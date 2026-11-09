from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType, StructField, StringType

def extract_payments(
    spark: SparkSession,
    path: str
) -> DataFrame:

    schema = StructType([
        StructField("payment_id", StringType(), True),
        StructField("recipient_id", StringType(), True),
        StructField("amount", StringType(), True),
        StructField("payment_date", StringType(), True),
        StructField("status", StringType(), True)
    ])

    return (
        spark.read
        .option("header", True)
        .schema(schema)
        .csv(path)
    )


def extract_recipients(
    spark: SparkSession,
    path: str   
) -> DataFrame:

    schema = StructType([
        StructField("recipient_id", StringType(), True),
        StructField("recipient_name", StringType(), True),
        StructField("region", StringType(), True),
        StructField("recipient_type", StringType(), True)
    ])

    return (
        spark.read
        .option("header", True)
        .schema(schema)
        .csv(path)
    )
