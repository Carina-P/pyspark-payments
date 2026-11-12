from pyspark.testing.utils import assertDataFrameEqual
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DecimalType, DateType
)
from decimal import Decimal
from datetime import date

from src.pipeline.main import run_pipeline
from src.pipeline.extract import extract_payments, extract_recipients


def test_iceberg_pipeline(iceberg_spark, iceberg_table, tmp_path):
    payments_file = tmp_path / "payments.csv"
    recipients_file = tmp_path / "recipients.csv"
    output_path = tmp_path / "processed"

    payments_file.write_text(
        "payment_id,recipient_id,amount,payment_date,status\n"
        "1,101,500,2026-01-01,completed\n"
        "2,102,300,2026-01-02,pending\n"
        "3,101,-100,2026-01-03,completed\n"
        "4,101,700,2026-02-01,completed\n"
    )

    recipients_file.write_text(
       "recipient_id,recipient_name,region,recipient_type\n"
        "101,Alfa Omsorg AB,Stockholm,Company\n"
        "102,Beta Service AB,Skane,Company\n"  
    )

    payments_df = extract_payments(iceberg_spark, str(payments_file))
    recipients_df = extract_recipients(iceberg_spark, str(recipients_file))

    expected_payments_schema = StructType([
        StructField("payment_id", IntegerType(), True),
        StructField("recipient_id", IntegerType(), True),
        StructField("amount", DecimalType(12,2), True),
        StructField("payment_date", DateType(), True),
        StructField("status", StringType(), True),
        StructField("recipient_name", StringType(), True),
        StructField("region", StringType(), True),
        StructField("recipient_type", StringType(), True)
    ])

    expected_payments_df = iceberg_spark.createDataFrame(
            [
                (
                    1, 101, Decimal("500.00"), date(2026, 1, 1), 
                    "completed", "Alfa Omsorg AB", "Stockholm", "Company"
                ),
                (
                    2, 102, Decimal("300.00"), date(2026, 1, 2),
                    "pending", "Beta Service AB", "Skane", "Company"
                ),
                (
                    4, 101, Decimal("700.00"), date(2026, 2, 1),
                    "completed", "Alfa Omsorg AB", "Stockholm", "Company"
                )
            ],
            expected_payments_schema
        )

    run_pipeline(
        iceberg_spark,
        str(payments_file),
        str(recipients_file),
        output_path,
        iceberg_enabled=True,
        iceberg_table=iceberg_table
    )

    actual_payments_df = iceberg_spark.sql(
        """
        SELECT * FROM IDENTIFIER(:table_name)
        """,
        args={"table_name": iceberg_table}
    )

    assertDataFrameEqual(actual_payments_df, expected_payments_df)

    # Check idempotency
    run_pipeline(
        iceberg_spark,
        str(payments_file),
        str(recipients_file),
        output_path,
        iceberg_enabled=True,
        iceberg_table=iceberg_table
    )

    actual_payments_df = iceberg_spark.sql(
        """
        SELECT * FROM IDENTIFIER(:table_name)
        """,
        args={"table_name": iceberg_table}
    )

    assertDataFrameEqual(actual_payments_df, expected_payments_df)

    # Check with actual update
    payments_file.write_text(
        "payment_id,recipient_id,amount,payment_date,status\n"
        "1,101,550,2026-01-01,completed\n"
        "2,102,300,2026-01-02,pending\n"
        "3,101,-100,2026-01-03,completed\n"
        "4,101,700,2026-02-01,completed\n"
        "5,102,900,2026-03-01,completed\n"
    )

    expected_payments_df = iceberg_spark.createDataFrame(
            [
                (
                    1, 101, Decimal("550.00"), date(2026, 1, 1), 
                    "completed", "Alfa Omsorg AB", "Stockholm", "Company"
                ),
                (
                    2, 102, Decimal("300.00"), date(2026, 1, 2),
                    "pending", "Beta Service AB", "Skane", "Company"
                ),
                (
                    4, 101, Decimal("700.00"), date(2026, 2, 1),
                    "completed", "Alfa Omsorg AB", "Stockholm", "Company"
                ),
                (
                    5, 102, Decimal("900.00"), date(2026, 3, 1),
                    "completed", "Beta Service AB", "Skane", "Company"
                )
            ],
            expected_payments_schema
        )

    run_pipeline(
        iceberg_spark,
        str(payments_file),
        str(recipients_file),
        output_path,
        iceberg_enabled=True,
        iceberg_table=iceberg_table
    )

    actual_payments_df = iceberg_spark.sql(
        """
        SELECT * FROM IDENTIFIER(:table_name)
        """,
        args={"table_name": iceberg_table}
    )

    assertDataFrameEqual(actual_payments_df, expected_payments_df)
