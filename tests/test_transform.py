from pyspark.testing.utils import assertDataFrameEqual, assertSchemaEqual
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DecimalType, DateType, LongType
)
from decimal import Decimal
from datetime import date

from src.pipeline.transform import transform_payments, aggregate_by_region


def test_tranform_payments(spark):

    payments_df = spark.createDataFrame(
        [
            ("1", "101", "500", "2026-01-01", "completed"),
            ("3", "105", "200", "2026-01-02", "completed"),
            ("6", "104", "400", "2026-04-01", "completed")
        ],
        [
            "payment_id",
            "recipient_id",
            "amount",
            "payment_date",
            "status",
        ],
    )

    recipients_df = spark.createDataFrame(
        [
            ("101","Alfa Omsorg AB","Stockholm","Company", 101),
            ("102","Beta Service AB","Skane","Company", 102),
            ("103","Gamma Assistans AB","Vastra Gotaland","Company", 103),  
            ("104","Delta Kulturforening","Stockholm","Nonprofit", 104),
            ("105","Epsilon Utbildning AB","Skane","Company", 105),
            ("106","Zeta Hjalporganisation","Vastra Gotaland","Nonprofit", 106),
        ],
        [
            "recipient_id",
            "recipient_name",
            "region",
            "recipient_type",
            "recipient_id_as_int"
        ]
    )

    expected_schema = StructType([
        StructField("payment_id", IntegerType(), True),
        StructField("recipient_id", IntegerType(), True),
        StructField("amount", DecimalType(12,2), True),
        StructField("payment_date", DateType(), True),
        StructField("status", StringType(), True),
        StructField("recipient_name", StringType(), True),
        StructField("region", StringType(), True),
        StructField("recipient_type", StringType(), True),
        StructField("year", IntegerType(), True),
        StructField("month", IntegerType(), True)
    ])

    expected_df = spark.createDataFrame(
        [
            (
                1, 101, Decimal("500.00"), date(2026, 1, 1), 
                "completed", "Alfa Omsorg AB", "Stockholm", "Company", 2026, 1
            ),
            (
                3, 105, Decimal("200.00"), date(2026, 1, 2),
                "completed", "Epsilon Utbildning AB","Skane","Company", 2026, 1
            ),
            (
                6, 104, Decimal("400.00"), date(2026, 4, 1),
                "completed", "Delta Kulturforening","Stockholm","Nonprofit", 2026, 4
            )
        ],
        expected_schema
    )

    actual_df = transform_payments(payments_df, recipients_df)

    assertDataFrameEqual(actual_df, expected_df)
    assertSchemaEqual(actual_df.schema, expected_schema)


def test_aggregate_by_region(spark):
    schema = StructType([
            StructField("recipient_id", IntegerType(), True),
            StructField("payment_id", IntegerType(), True),
            StructField("amount", DecimalType(12,2), True),
            StructField("payment_date", DateType(), True),
            StructField("status", StringType(), True),
            StructField("recipient_name", StringType(), True),
            StructField("region", StringType(), True),
            StructField("recipient_type", StringType(), True),
            StructField("year", IntegerType(), True),
            StructField("month", IntegerType(), True)
        ])
    
    input_df = spark.createDataFrame(
        [
            (
                101, 1, Decimal("500.00"), date(2026, 1, 1),
                "completed", "Alfa Omsorg AB", "Stockholm", "Company", 2026, 1
            ),
            (
                105, 3, Decimal("200.00"), date(2026, 1, 2),
                "completed", "Epsilon Utbildning AB","Skane","Company", 2026, 1
            ),
            (
                104, 6, Decimal("300.00"), date(2026, 4, 1),
                "pending", "Delta Kulturforening","Stockholm","Nonprofit", 2026, 4
            ),
            (
                104, 6, Decimal("400.00"), date(2026, 4, 1),
                "completed", "Delta Kulturforening","Stockholm","Nonprofit", 2026, 4
            ),

        ],
        schema
    )

    expected_schema =  StructType([
            StructField("region", StringType(), True),
            StructField("total_amount", DecimalType(22, 2), True),
            StructField("total_payments", LongType(), False),
            StructField("avg_amount", DecimalType(16, 6), True)
        ])

    expected_df = spark.createDataFrame(
        [
            ("Stockholm", Decimal("900.00"), 2, Decimal("450.000000")),
            ("Skane", Decimal("200.00"), 1, Decimal("200.00"))
        ],
        expected_schema
    )

    actual_df = aggregate_by_region(input_df)

    assertDataFrameEqual(actual_df, expected_df)
    assertSchemaEqual(actual_df.schema, expected_schema)
