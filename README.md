# PySpark Payments ETL Pipeline

A modular ETL pipeline built with Python and Apache Spark (PySpark) to validate payment transactions, enrich them with recipient information, and generate regional payment statistics.

The project demonstrates data quality checks, Spark DataFrame transformations, Parquet storage, partitioning, automated testing, and idempotent batch processing.

## Features

- **CSV ingestion:** Reads payments and recipient reference data.
- **Payment validation:** Checks field formats, positive amounts, permitted statuses, duplicate payment IDs, and recipient existence.
- **Traceable rejections:** Preserves original values and records multiple validation errors for rejected payments.
- **Reference-data validation:** Stops processing when recipient IDs are invalid or duplicated.
- **Transformations:** Casts validated fields to appropriate types and enriches payments with recipient information.
- **Regional aggregates:** Calculates total amount, transaction count, and average amount for completed payments.
- **Parquet output:** Stores processed payments partitioned by year and month, alongside rejected payments and regional aggregates.
- **Automated tests:** Uses pytest and PySpark's DataFrame comparison helpers for unit and integration tests.
- **Continuous integration**: GitHub Actions runs automated tests on pushes and pull requests.

## Pipeline architecture

```text
CSV payments + recipients
          |
          v
        Extract
          |
          v
  Validate recipient lookup
          |
          v
    Validate payments ------> Invalid payments (Parquet)
          |
          v
     Valid payments
          |
          v
  Transform and enrich -----> Processed payments (partitioned Parquet)
          |
          v
 Aggregate completed payments by region
          |
          v
    Regional aggregates (Parquet)
```

## Project structure

```text
data/
  raw/                       # Sample input CSV files
  processed/                 # Generated Parquet outputs (not tracked by Git)
src/
  pipeline/
    exceptions.py            # Custom pipeline exceptions
    extract.py               # Read source CSV files
    validate.py              # Payment and recipient validation
    transform.py             # Type conversion, enrichment, aggregation
    load.py                  # Write Parquet outputs
    main.py                  # Pipeline orchestration
tests/
  conftest.py                # Shared SparkSession fixture
  test_validate.py           # Validation tests
  test_transform.py          # Transformation and aggregation tests
  test_pipeline.py           # Integration and idempotency tests
requirements.txt
README.md
```

## Data validation

Payments are initially read as strings so that malformed source values can be preserved and reported, rather than lost during parsing.

| Field | Rule |
|---|---|
| `payment_id` | Must be a valid integer and unique within the input batch |
| `recipient_id` | Must be a valid integer and exist in the recipient reference data |
| `amount` | Must be positive and exactly representable as Decimal(12,2), without rounding. Trailing zeros are allowed. |
| `payment_date` | Must be a valid date |
| `status` | Must be `completed`, `pending`, or `failed` |

Payment IDs are normalized numerically for duplicate detection (for example, `"101"` and `"0101"` are treated as duplicates). All occurrences of a duplicated ID are rejected, including identical rows.

Rejected payments retain their original values and a `validation_errors` field listing the applicable problems. The recipient lookup is checked for invalid and duplicate IDs before payment processing.

## Outputs

Running the pipeline creates three Parquet datasets under `data/processed/`:

| Directory | Contents |
|---|---|
| `payments/` | Validated payments enriched with recipient information, partitioned by `year` and `month` |
| `invalid_rows/` | Rejected payments with validation errors |
| `aggregated_by_region/` | Total amount, count, and average amount by region for `completed` payments |

## Getting started

### Prerequisites

This project was developed on macOS with:

- **Python 3.13.18**
- **PySpark 4.3.0**
- **Java JDK 17**
- `pip`

Ensure Java is installed and available to Spark.

### Install

```bash
git clone https://github.com/Carina-P/pyspark-payments.git
cd pyspark-payments
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### Run the pipeline

From the project root:

```bash
python -m src.pipeline.main
```

The default run reads sample CSV files from `data/raw/` and writes Parquet outputs to `data/processed/`.

## Tests

Run the test suite from the project root:

```bash
python -m pytest -v
```

The tests cover field validation, payment-ID duplication, recipient lookup checks, multiple validation errors, transformations, regional aggregation, end-to-end CSV-to-Parquet processing, and idempotency across repeated runs.

Integration tests use pytest's `tmp_path` fixture to isolate input and output files. PySpark's `assertDataFrameEqual()` and `assertSchemaEqual()` are used to verify results.

## Design decisions and limitations

- **Idempotent batch runs:** Successful repeated runs with identical input use overwrite-mode output to avoid accumulating duplicate records.
- **Error classification:** Custom exceptions distinguish data-quality failures from failures encountered during Parquet writes.
- **Full-batch processing:** The pipeline currently processes the complete input dataset. It does not yet support incremental upserts or checking new batches against already stored payment IDs.
- **Write atomicity:** Separate Parquet writes are not one atomic transaction. An interrupted run may leave partially updated output datasets.

## Future improvements

- Incremental ingestion and upserts
- Apache Iceberg for transactional table updates
- Pipeline metrics and more structured logging

## Technologies

Python · PySpark · Spark SQL · Parquet · pytest · Git · GitHub
