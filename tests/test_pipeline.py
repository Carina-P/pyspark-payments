from pyspark.testing.utils import assertDataFrameEqual, assertSchemaEqual
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DecimalType, DateType, LongType
)
from decimal import Decimal
from datetime import date

from src.pipeline.main import run_pipeline
from src.pipeline.extract import extract_payments, extract_recipients
from src.pipeline.load import load


def test_pipeline_files(spark, tmp_path):
    payments_file = tmp_path / "payments.csv"
    recipients_file = tmp_path / "recipients.csv"
    output_path = tmp_path / "processed" / "payments"

    payments_file.write_text(
        "payment_id,recipient_id,amount,payment_date,status\n"
        "1,101,500,2026-01-01,completed\n"
        "2,102,-100,2026-01-02,completed\n"
    )

    recipients_file.write_text(
       "recipient_id,recipient_name,region,recipient_type\n"
        "101,Alfa Omsorg AB,Stockholm,Company\n"
        "102,Beta Service AB,Skane,Company\n"  
    )

    payments_df = extract_payments(spark, str(payments_file))
    recipients_df = extract_recipients(spark, str(recipients_file))

    assert payments_df.count() == 2
    assert recipients_df.count() == 2

    load(
        payments_df,
        str(output_path)
    )

    result_df = spark.read.parquet(str(output_path))

    assertDataFrameEqual(result_df, payments_df)
    assertSchemaEqual(result_df.schema, payments_df.schema)


def test_pipeline(spark, tmp_path):
    payments_file = tmp_path / "payments.csv"
    recipients_file = tmp_path / "recipients.csv"
    output_path = tmp_path / "processed"

    payments_file.write_text(
        "payment_id,recipient_id,amount,payment_date,status\n"
        "1,0101,500,2026-01-01,completed\n"
        "2,102,-100,2026-01-02,completed\n"
        "3,999,200,2026-02-01,completed\n"
        "4,101,300,2026-03-03,pending\n"
    )

    recipients_file.write_text(
        "recipient_id,recipient_name,region,recipient_type\n"
        "101,Alfa Omsorg AB,Stockholm,Company\n"
        "102,Beta Service AB,Skane,Company\n"  
    )

    expected_payments_schema = StructType([
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

    expected_payments_df = spark.createDataFrame(
            [
                (
                    1, 101, Decimal("500.00"), date(2026, 1, 1), 
                    "completed", "Alfa Omsorg AB", "Stockholm", "Company", 2026, 1
                ),
                (
                    4, 101, Decimal("300.00"), date(2026, 3, 3),
                    "pending", "Alfa Omsorg AB", "Stockholm", "Company", 2026, 3
                )
            ],
            expected_payments_schema
        )

    expected_invalid_rows_df = spark.createDataFrame(
            [
                ("2", "102", "-100", "2026-01-02", "completed", "Invalid amount"),
                ("3", "999", "200", "2026-02-01", "completed","Unknown recipient_id"),
            ],
            [
                "payment_id",
                "recipient_id",
                "amount",
                "payment_date",
                "status",
                "validation_errors"
            ],
        )

    expected_agg_schema =  StructType([
            StructField("region", StringType(), True),
            StructField("total_amount", DecimalType(22, 2), True),
            StructField("total_payments", LongType(), False),
            StructField("avg_amount", DecimalType(16, 6), True)
        ])
    
    expected_agg_df = spark.createDataFrame(
        [
            ("Stockholm", Decimal("500.00"), 1, Decimal("500.000000"))
        ],
        expected_agg_schema
    )

    run_pipeline(
        spark,
        str(payments_file),
        str(recipients_file),
        output_path
    )

    actual_payments_df = spark.read.parquet(str(output_path / "payments"))
    actual_invalid_rows_df = spark.read.parquet(str(output_path / "invalid_rows"))
    actual_aggregated_by_region_df = spark.read.parquet(str(output_path / "aggregated_by_region"))

    assertDataFrameEqual(actual_payments_df, expected_payments_df)
    assertSchemaEqual(actual_payments_df.schema, expected_payments_schema)

    assertDataFrameEqual(actual_invalid_rows_df, expected_invalid_rows_df)
    assertSchemaEqual(actual_invalid_rows_df.schema, expected_invalid_rows_df.schema)

    assertDataFrameEqual(actual_aggregated_by_region_df, expected_agg_df)
    assertSchemaEqual(actual_aggregated_by_region_df.schema, expected_agg_schema)

    assert (
        output_path / "payments" / "year=2026" / "month=1"
    ).is_dir()

    assert (
        output_path / "payments" / "year=2026" / "month=3"
    ).is_dir()


def test_idempotency_by_running_twice(spark, tmp_path):
    payments_file = tmp_path / "payments.csv"
    recipients_file = tmp_path / "recipients.csv"
    output_path = tmp_path / "processed"

    payments_file.write_text(
        "payment_id,recipient_id,amount,payment_date,status\n"
        "1,101,500,2026-01-01,completed\n"
        "2,102,-100,2026-01-02,completed\n"
        "3,999,200,2026-02-01,completed\n"
        "4,101,300,2026-03-03,pending\n"
    )

    recipients_file.write_text(
        "recipient_id,recipient_name,region,recipient_type\n"
        "101,Alfa Omsorg AB,Stockholm,Company\n"
        "102,Beta Service AB,Skane,Company\n"  
    )

    # First run
    run_pipeline(
        spark,
        str(payments_file),
        str(recipients_file),
        output_path
    )

    payments_df1 = spark.read.parquet(str(output_path / "payments"))
    invalid_df1 = spark.read.parquet(str(output_path / "invalid_rows"))
    aggregated_df1 = spark.read.parquet(str(output_path / "aggregated_by_region"))

    # Save actual data before files are overwritten
    payments_rows1 = payments_df1.collect()
    invalid_rows1 = invalid_df1.collect()
    aggregated_rows1 = aggregated_df1.collect()

    # Second run
    run_pipeline(
        spark,
        str(payments_file),
        str(recipients_file),
        output_path
    )

    payments_df2 = spark.read.parquet(str(output_path / "payments"))
    invalid_df2 = spark.read.parquet(str(output_path / "invalid_rows"))
    aggregated_df2 = spark.read.parquet(str(output_path / "aggregated_by_region"))

    # Compare results
    assertDataFrameEqual(
        spark.createDataFrame(payments_rows1, payments_df1.schema),
        payments_df2
    )

    assertDataFrameEqual(
        spark.createDataFrame(invalid_rows1, invalid_df1.schema),
        invalid_df2
    )

    assertDataFrameEqual(
        spark.createDataFrame(aggregated_rows1, aggregated_df1.schema),
        aggregated_df2
    )
