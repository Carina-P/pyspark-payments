from pyspark.testing.utils import assertDataFrameEqual
import pytest

from src.pipeline.exceptions import DataQualityError

from src.pipeline.validate import (
    validate_columns,
    validate_duplicates,
    validate_recipients,
    validate_recipients_lookup,
    check_validation,
    validate_payments,
)


def test_validate_columns(spark):
    input_df = spark.createDataFrame(
        [
            ("1", "101", "500", "2026-01-01", "completed"),
            ("2", "102", "-100", "2026-01-02", "completed"),
            ("abc", "101", "500", "2026-01-01", "completed"),
            ("3", "xyz", "200", "2026-01-02", "completed"),
            ("4", "103", "xyz", "2026-01-03", "completed"),
            ("5", "104", "300", "fel", "completed"),
            ("6", "105", "400", "2026-04-01", "unknown")
        ],
        [
            "payment_id",
            "recipient_id",
            "amount",
            "payment_date",
            "status",
        ],
    )

    expected_df = spark.createDataFrame(
        [
            ("1", "101", "500", "2026-01-01", "completed", True, True, True, True, True),
            ("2", "102", "-100", "2026-01-02", "completed", True, True, False, True, True),
            ("abc", "101", "500", "2026-01-01", "completed",False, True, True, True, True),
            ("3", "xyz", "200", "2026-01-02", "completed",True, False, True, True, True),
            ("4", "103", "xyz", "2026-01-03", "completed",True, True, False, True, True),
            ("5", "104", "300", "fel", "completed",True, True, True, False, True),
            ("6", "105", "400", "2026-04-01", "unknown",True, True, True, True, False)
        ],
        [
            "payment_id",
            "recipient_id",
            "amount",
            "payment_date",
            "status",
            "valid_payment_id",
            "valid_recipient_id",
            "valid_amount",
            "valid_date",
            "valid_status"
        ],
    )
    
    actual_df = validate_columns(input_df)

    assertDataFrameEqual(actual_df, expected_df)


def test_validate_duplicates(spark):
    input_df = spark.createDataFrame(
        [
            ("1", "101", "500", "2026-01-01", "completed",True, True, True, True, True),
            ("2", "102", "-100", "2026-01-02", "completed",True, True, False, True, True),
            ("01", "101", "500", "2026-01-01", "completed", True, True, True, True, True),
            ("3", "xyz", "200", "2026-01-02", "completed",True, False, True, True, True),
            (None, "103", "xyz", "2026-01-03", "completed",False, True, False, True, True),
            (None, "104", "300", "fel", "completed",False, True, True, False, True),
            ("3", "105", "400", "2026-01-04", "unknown",True, True, True, True, False)
        ],
        [
            "payment_id",
            "recipient_id",
            "amount",
            "payment_date",
            "status",
            "valid_payment_id",
            "valid_recipient_id",
            "valid_amount",
            "valid_date",
            "valid_status",
        ],
    )

    expected_df = spark.createDataFrame(
        [
            ("1", "101", "500", "2026-01-01", "completed", True, True, True, True, True, 2),
            ("2", "102", "-100", "2026-01-02", "completed", True, True, False, True, True, 1),
            ("01", "101", "500", "2026-01-01", "completed", True, True, True, True, True, 2),
            ("3", "xyz", "200", "2026-01-02", "completed",True, False, True, True, True, 2),
            (None, "103", "xyz", "2026-01-03", "completed",False, True, False, True, True, 2),
            (None, "104", "300", "fel", "completed",False, True, True, False, True, 2),
            ("3", "105", "400", "2026-01-04", "unknown",True, True, True, True, False, 2)
        ],
        [
            "payment_id",
            "recipient_id",
            "amount",
            "payment_date",
            "status",
            "valid_payment_id",
            "valid_recipient_id",
            "valid_amount",
            "valid_date",
            "valid_status",
            "id_occurrences"
        ],
    )
    
    actual_df = validate_duplicates(input_df)

    assertDataFrameEqual(
        actual_df,
        expected_df,
        ignoreNullable=True
    )


def test_validate_recipients(spark):
    input_df = spark.createDataFrame(
        [
            ("1", "101", "500", "2026-01-01", "completed", True, True, True, True, True, 2),
            ("2", "102", "-100", "2026-01-02", "completed", True, True, False, True, True, 1),
            ("01", "0101", "500", "2026-01-01", "completed", True, True, True, True, True, 2),
            ("3", "xyz", "200", "2026-01-02", "completed",True, False, True, True, True, 2),
            (None, "103", "xyz", "2026-01-03", "completed",False, True, False, True, True, 2),
            (None, "104", "300", "fel", "completed",False, True, True, False, True, 2),
            ("3", "107", "400", "2026-01-04", "unknown",True, True, True, True, False, 2),
            ("4", None, "400", "2026-01-04", "unknown",True, False, True, True, False, 1)
        ],
        [
            "payment_id",
            "recipient_id",
            "amount",
            "payment_date",
            "status",
            "valid_payment_id",
            "valid_recipient_id",
            "valid_amount",
            "valid_date",
            "valid_status",
            "id_occurrences"
        ],
    )

    recipient_df = spark.createDataFrame(
        [
            ("101","Alfa Omsorg AB","Stockholm","Company"),
            ("102","Beta Service AB","Skane","Company"),
            ("103","Gamma Assistans AB","Vastra Gotaland","Company"),  
            ("104","Delta Kulturforening","Stockholm","Nonprofit"),
            ("105","Epsilon Utbildning AB","Skane","Company"),
            ("106","Zeta Hjalporganisation","Vastra Gotaland","Nonprofit"),
            ("101","Alfa Omsorg AB","Stockholm","Company")
        ],
        [
            "recipient_id",
            "recipient_name",
            "region",
            "recipient_type"
        ]
    )

    expected_df = spark.createDataFrame(
        [
            ("1", "101", "500", "2026-01-01", "completed", True, True, True, True, True, 2, True),
            ("2", "102", "-100", "2026-01-02", "completed", True, True, False, True, True, 1, True),
            ("01", "0101", "500", "2026-01-01", "completed", True, True, True, True, True, 2, True),
            ("3", "xyz", "200", "2026-01-02", "completed",True, False, True, True, True, 2, False),
            (None, "103", "xyz", "2026-01-03", "completed",False, True, False, True, True, 2, True),
            (None, "104", "300", "fel", "completed",False, True, True, False, True, 2, True),
            ("3", "107", "400", "2026-01-04", "unknown",True, True, True, True, False, 2, False),
            ("4", None, "400", "2026-01-04", "unknown",True, False, True, True, False, 1, False),
        ],
        [
            "payment_id",
            "recipient_id",
            "amount",
            "payment_date",
            "status",
            "valid_payment_id",
            "valid_recipient_id",
            "valid_amount",
            "valid_date",
            "valid_status",
            "id_occurrences",
            "existing_recipient",
        ],
    )
    
    actual_df = validate_recipients(input_df, recipient_df)

    assertDataFrameEqual(
        actual_df,
        expected_df,
        ignoreNullable=True
    )

    assert actual_df.count() == input_df.count()


def test_validate_recipients_lookup_invalid_recipient_id_raises_error(spark):
    recipients_df = spark.createDataFrame(
        [
            ("xyz","Alfa Omsorg AB","Stockholm","Company"),
            ("102","Beta Service AB","Skane","Company"),
            ("0101","Alfa Omsorg AB","Stockholm","Company")
        ],
        [
            "recipient_id",
            "recipient_name",
            "region",
            "recipient_type"
        ]
    )

    with pytest.raises(DataQualityError) as exc_info:
        validate_recipients_lookup(recipients_df)
    assert str(exc_info.value) == "Recipient_id with invalid value in recipients data"


def test_validate_recipients_lookup_duplett_raises_error(spark):
    recipients_df = spark.createDataFrame(
        [
            ("101","Alfa Omsorg AB","Stockholm","Company"),
            ("102","Beta Service AB","Skane","Company"),
            ("0101","Alfa Omsorg AB","Stockholm","Company")
        ],
        [
            "recipient_id",
            "recipient_name",
            "region",
            "recipient_type"
        ]
    )

    with pytest.raises(DataQualityError) as exc_info:
        validate_recipients_lookup(recipients_df)
    assert str(exc_info.value) == "Duplicate recipient_id: 101"


def test_validate_recipients_lookup_valid_data(spark):
    recipients_df = spark.createDataFrame(
        [
            ("101", "Alfa Omsorg AB", "Stockholm", "Company"),
            ("102", "Beta Service AB", "Skane", "Company"),
            ("103", "Gamma Assistans AB", "Stockholm", "Company"),
        ],
        [
            "recipient_id",
            "recipient_name",
            "region",
            "recipient_type",
        ]
    )

    validate_recipients_lookup(recipients_df)


def test_check_validation_invalid_payment_id(spark):
    input_df = spark.createDataFrame(
        [
            ("1", "101", "500", "2026-01-01", "completed", True, True, True, True, True, 1, True),
            ("x", "102", "100", "2026-01-02", "completed", False, True, True, True, True, 1, True),
        ],
        [
            "payment_id",
            "recipient_id",
            "amount",
            "payment_date",
            "status",
            "valid_payment_id",
            "valid_recipient_id",
            "valid_amount",
            "valid_date",
            "valid_status",
            "id_occurrences",
            "existing_recipient",
        ],
    )

    expected_df = spark.createDataFrame(
        [
            ("1", "101", "500", "2026-01-01", "completed", "", True),
            ("x", "102", "100", "2026-01-02", "completed", "Invalid payment_id", False),
        ],
        [
            "payment_id",
            "recipient_id",
            "amount",
            "payment_date",
            "status",
            "validation_errors",
            "is_valid"  
        ]
    )

    actual_df = check_validation(input_df)

    assertDataFrameEqual(
        actual_df,
        expected_df,
        ignoreNullable=True
    )


def test_check_validation_two_invalid_columns(spark):
    input_df = spark.createDataFrame(
        [
            ("1", "101", "500", "2026-01-01", "completed", True, True, True, True, True, 1, True),
            ("x", "102", "-100", "2026-01-02", "completed", False, True, False, True, True, 1, True),
        ],
        [
            "payment_id",
            "recipient_id",
            "amount",
            "payment_date",
            "status",
            "valid_payment_id",
            "valid_recipient_id",
            "valid_amount",
            "valid_date",
            "valid_status",
            "id_occurrences",
            "existing_recipient",
        ],
    )

    expected_df = spark.createDataFrame(
        [
            ("1", "101", "500", "2026-01-01", "completed", "", True),
            ("x", "102", "-100", "2026-01-02", "completed", "Invalid payment_id, Invalid amount", False),
        ],
        [
            "payment_id",
            "recipient_id",
            "amount",
            "payment_date",
            "status",
            "validation_errors",
            "is_valid"  
        ]
    )

    actual_df = check_validation(input_df)

    assertDataFrameEqual(
        actual_df,
        expected_df,
        ignoreNullable=True
    )


def test_check_validation_five_invalid_columns(spark):
    input_df = spark.createDataFrame(
        [
            ("1", "101", "500", "2026-01-01", "completed", True, True, True, True, True, 1, True),
            ("x", "xyz", "-100", "fel-date-in", "unknown", False, False, False, False, False, 1, False),
        ],
        [
            "payment_id",
            "recipient_id",
            "amount",
            "payment_date",
            "status",
            "valid_payment_id",
            "valid_recipient_id",
            "valid_amount",
            "valid_date",
            "valid_status",
            "id_occurrences",
            "existing_recipient",
        ],
    )

    expected_df = spark.createDataFrame(
        [
            ("1", "101", "500", "2026-01-01", "completed", "", True),
            (
                "x", "xyz", "-100", "fel-date-in", "unknown", 
                "Invalid payment_id, Invalid recipient_id, Invalid amount, Invalid payment_date, Invalid status", False),
        ],
        [
            "payment_id",
            "recipient_id",
            "amount",
            "payment_date",
            "status",
            "validation_errors",
            "is_valid"  
        ]
    )

    actual_df = check_validation(input_df)

    assertDataFrameEqual(
        actual_df,
        expected_df,
        ignoreNullable=True
    )


def test_check_validation_dubletts(spark):
    input_df = spark.createDataFrame(
        [
            ("1", "101", "500", "2026-01-01", "completed", True, True, True, True, True, 2, True),
            ("1", "101", "500", "2026-01-01", "completed", True, True, True, True, True, 2, True),
            (None, "102", "100", "2026-01-02", "completed", False, True, True, True, True, 2, True),
            (None, "102", "100", "2026-01-02", "completed", False, True, True, True, True, 2, True),
        ],
        [
            "payment_id",
            "recipient_id",
            "amount",
            "payment_date",
            "status",
            "valid_payment_id",
            "valid_recipient_id",
            "valid_amount",
            "valid_date",
            "valid_status",
            "id_occurrences",
            "existing_recipient",
        ],
    )

    expected_df = spark.createDataFrame(
        [
            ("1", "101", "500", "2026-01-01", "completed", "Duplicate payment_id", False),
            ("1", "101", "500", "2026-01-01", "completed", "Duplicate payment_id", False),
            (None, "102", "100", "2026-01-02", "completed", "Invalid payment_id", False),
            (None, "102", "100", "2026-01-02", "completed", "Invalid payment_id", False),
        ],
        [
            "payment_id",
            "recipient_id",
            "amount",
            "payment_date",
            "status",
            "validation_errors",
            "is_valid"  
        ]
    )

    actual_df = check_validation(input_df)

    assertDataFrameEqual(
        actual_df,
        expected_df,
        ignoreNullable=True
    )


def test_check_validation_unknown_recipient_id(spark):    
    input_df = spark.createDataFrame(
        [
            ("1", "101", "500", "2026-01-01", "completed", True, True, True, True, True, 1, True),
            ("2", "999", "100", "2026-01-02", "completed", True, True, True, True, True, 1, False),
        ],
        [
            "payment_id",
            "recipient_id",
            "amount",
            "payment_date",
            "status",
            "valid_payment_id",
            "valid_recipient_id",
            "valid_amount",
            "valid_date",
            "valid_status",
            "id_occurrences",
            "existing_recipient",
        ],
    )

    expected_df = spark.createDataFrame(
        [
            ("1", "101", "500", "2026-01-01", "completed", "", True),
            ("2", "999", "100", "2026-01-02", "completed", "Unknown recipient_id", False),
        ],
        [
            "payment_id",
            "recipient_id",
            "amount",
            "payment_date",
            "status",
            "validation_errors",
            "is_valid"  
        ]
    )

    actual_df = check_validation(input_df)

    assertDataFrameEqual(
        actual_df,
        expected_df,
        ignoreNullable=True
    )


def test_validate_payments(spark):
    payments_df = spark.createDataFrame(
            [
                ("1", "101", "500", "2026-01-01", "completed"),
                ("2", "102", "-100", "2026-01-02", "completed"),
                ("abc", "0101", "500", "2026-01-01", "completed"),
                ("3", "105", "200", "2026-01-02", "completed"),
                ("2", "103", "xyz", "2026-01-03", "completed"),
                ("5", "999", "300", "fel", "unknown"),
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
            ("101","Alfa Omsorg AB","Stockholm","Company"),
            ("102","Beta Service AB","Skane","Company"),
            ("103","Gamma Assistans AB","Vastra Gotaland","Company"),  
            ("104","Delta Kulturforening","Stockholm","Nonprofit"),
            ("105","Epsilon Utbildning AB","Skane","Company"),
            ("106","Zeta Hjalporganisation","Vastra Gotaland","Nonprofit"),
        ],
        [
            "recipient_id",
            "recipient_name",
            "region",
            "recipient_type"
        ]
    )

    expected_valid_payments_df = spark.createDataFrame(
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

    expected_invalid_rows_df = spark.createDataFrame(
        [
            ("2", "102", "-100", "2026-01-02", "completed", "Invalid amount, Duplicate payment_id"),
            ("abc", "0101", "500", "2026-01-01", "completed", "Invalid payment_id"),
            ("2", "103", "xyz", "2026-01-03", "completed","Invalid amount, Duplicate payment_id"),
            ("5", "999", "300", "fel", "unknown","Invalid payment_date, Invalid status, Unknown recipient_id"),
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

    actual_payment_df, actual_invalid_rows_df = validate_payments(payments_df, recipients_df)

    assert payments_df.count() == actual_payment_df.count() + actual_invalid_rows_df.count()

    assertDataFrameEqual(
        actual_payment_df,
        expected_valid_payments_df,
        ignoreNullable=True
    )    

    assertDataFrameEqual(
        actual_invalid_rows_df,
        expected_invalid_rows_df,
        ignoreNullable=True
    )
