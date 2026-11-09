from pyspark.sql import SparkSession
from pathlib import Path
import logging

from src.pipeline.exceptions import DataQualityError, PipelineWriteError
from src.pipeline.extract import extract_payments, extract_recipients
from src.pipeline.validate import validate_payments, validate_recipients_lookup
from src.pipeline.transform import transform
from src.pipeline.load import load

logger = logging.getLogger(__name__)


def main():
    spark = (
        SparkSession.builder
        .appName("Payments ETL")
        .master("local[*]")
        .getOrCreate()
    )

    try:
        logger.info("Starting payments pipeline")
        run_pipeline(
            spark,
            "data/raw/payments.csv",
            "data/raw/recipients.csv",
            Path("data/processed")
        )

        logger.info("Payments pipeline completed")
    except DataQualityError as e:
        logger.error("Data quality error: %s", e)
        raise

    except PipelineWriteError as e:
        logger.error("Pipeline write error: %s", e)
        raise

    except Exception:
        logger.exception("Unexpected pipeline error")
        raise

    finally:
        spark.stop()


    # try:
    #     logger.info("Starting payments pipeline")
    
    #     payments_df = extract_payments(spark, "data/raw/payments.csv")
    #     recipients_df = extract_recipients(spark, "data/raw/recipients.csv")

    #     validate_recipients_lookup(recipients_df)

    #     valid_payments_df, invalid_rows_df = validate_payments(payments_df, recipients_df)
    #     load(invalid_rows_df, "data/processed/invalid_rows")

    #     transformed_payments_df, aggregated_by_region_df = transform(valid_payments_df, recipients_df)
    #     load(transformed_payments_df, "data/processed/payments", partition_columns=("year", "month"))
    #     load(aggregated_by_region_df, "data/processed/aggregated_by_region")

    #     logger.info("Payments pipeline completed")
    
    # except DataQualityError as e:
    #     logger.error("Data quality error: %s", e)
    #     raise

    # except PipelineWriteError as e:
    #     logger.error("Pipeline write error: %s", e)
    #     raise

    # except Exception:
    #     logger.exception("Unexpected pipeline error")
    #     raise
    
    # finally:
    #     spark.stop()


def run_pipeline(
    spark: SparkSession,
    payments_path: str,
    recipients_path: str,
    output_path: Path
):

    payments_df = extract_payments(spark, payments_path)
    recipients_df = extract_recipients(spark, recipients_path)

    validate_recipients_lookup(recipients_df)

    valid_payments_df, invalid_rows_df = validate_payments(payments_df, recipients_df)
    load(invalid_rows_df, str(output_path / "invalid_rows"))

    transformed_payments_df, aggregated_by_region_df = transform(valid_payments_df, recipients_df)
    load(transformed_payments_df, str(output_path / "payments"), partition_columns=("year", "month"))
    load(aggregated_by_region_df, str(output_path / "aggregated_by_region"))


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()
