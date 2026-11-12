import pytest
import uuid
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def iceberg_spark(spark):
    return spark


@pytest.fixture(scope="function")
def iceberg_table(iceberg_spark):
    table_name = f"demo.payments.test_{uuid.uuid4().hex[:8]}"

    try:
        iceberg_spark.sql(
            """
            CREATE TABLE IDENTIFIER(:table_name) (
                payment_id INT,
                recipient_id INT,
                amount DECIMAL(12,2),
                payment_date DATE,
                status STRING,
                recipient_name STRING,
                region STRING,
                recipient_type STRING
            )
            USING ICEBERG
            PARTITIONED BY (months(payment_date))
            """,
            args={"table_name": table_name}
        )
        print("Created Iceberg table:", table_name)

        iceberg_spark.sql(
            "SHOW TABLES IN demo.payments"
        ).show()

        yield table_name
    
    finally:
        iceberg_spark.sql(
            "DROP TABLE IF EXISTS IDENTIFIER(:table_name)",
            args={"table_name": table_name}
        )
