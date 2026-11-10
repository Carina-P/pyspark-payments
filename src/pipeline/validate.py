from pyspark.sql import DataFrame, Window, functions as F

from src.pipeline.exceptions import DataQualityError


def validate_recipients_lookup(
        recipients_df: DataFrame
) -> DataFrame:

    normalized_recipient_id_df = recipients_df.withColumn(
        "recipient_id_as_int",
        F.expr("try_cast(recipient_id as int)")
    )

    # recipient_id == NULL is invalid
    if (
        normalized_recipient_id_df.select(
            "recipient_id_as_int"
        ).where(
            F.col("recipient_id_as_int").isNull()
        ).limit(1).count() > 0
    ):
        raise DataQualityError(
            "Recipient_id with invalid value in recipients data"
        )

    # Duplicate recipient_id invalid
    duplicate = (
        normalized_recipient_id_df
        .groupBy("recipient_id_as_int")
        .count()
        .filter(F.col("count") > 1)
        .first()
    )
    if duplicate:
        raise DataQualityError(
            f"Duplicate recipient_id: {duplicate['recipient_id_as_int']}"
        )
    return normalized_recipient_id_df


def validate_payments(
    payments_df: DataFrame,
    recipients_lookup_df: DataFrame
) -> tuple[DataFrame, DataFrame]:
    """
    Validate the payments DataFrame by validating columns,
    checking for duplicates, and checking recipients.

    Args:
        payments_df (DataFrame): Raw payments DataFrame.
        recipients_df (Dataframe): Recipient reference Dataframe

    Returns:
        tuple[DataFrame, DataFrame]: A tuple containing the validated payments DataFrame and any invalid rows.
    """
    # Validate columns
    validated_columns_df = validate_columns(payments_df)

    # Check for duplicates
    duplicate_checked_df = validate_duplicates(validated_columns_df)

    # Check recipients
    recipient_checked_df = validate_recipients(duplicate_checked_df, recipients_lookup_df)

    # Final validation
    validated_payments_df = check_validation(recipient_checked_df)


    # Filter out valid and invalid rows
    valid_payments_df = validated_payments_df.filter(F.col("is_valid")).drop("is_valid", "validation_errors")
    invalid_rows_df = validated_payments_df.filter(~F.col("is_valid")).drop("is_valid")

    return valid_payments_df, invalid_rows_df


def validate_columns(
    payments_df: DataFrame
) -> DataFrame:
    """
    Validate the columns of the payments DataFrame.

    Args:
        payments_df (DataFrame): The input payments DataFrame.

    Returns:
        DataFrame: The validated payments DataFrame with additional validation columns.
    """
    return (
        payments_df
        .withColumn(
            "valid_payment_id", F.expr("try_cast(payment_id as int)").isNotNull()
        ).withColumn(
            "valid_recipient_id", F.expr("try_cast(recipient_id as int)").isNotNull()
        ).withColumn(
            "valid_amount",
            F.coalesce(
                F.expr("try_cast(amount as decimal(12,2)) > 0")
                & F.col("amount").rlike(r"^[0-9]+(\.[0-9]{1,2}0*)?$"),
                F.lit(False)
            )
        ).withColumn(
            "valid_date", F.expr("try_to_timestamp(payment_date, 'yyyy-MM-dd')").isNotNull()
        ).withColumn(
            "valid_status",
            F.coalesce(
                F.col("status").isin("completed", "pending", "failed"),
                F.lit(False)
            )
        )
    )


def validate_duplicates(payments_df: DataFrame) -> DataFrame:
    """
    Check for duplicates in the payments DataFrame based on payment_id.

    Args:
        payments_df (DataFrame): The payments DataFrame.

    Returns:
        DataFrame: The payments DataFrame with an additional column indicating duplicate occurrences.
    """

    window_spec = Window.partitionBy("casted_payment_id")
    
    return (
        payments_df
         .withColumn(
            "casted_payment_id",
            F.coalesce(
                F.expr("try_cast(payment_id as int)").cast("string"),
                F.col("payment_id")
            )
        )
        .withColumn(
            "id_occurrences",
            F.count("*").over(window_spec)
        )
        .drop("casted_payment_id")
    )


def validate_recipients(
    payments_df: DataFrame,
    recipients_lookup_df: DataFrame
) -> DataFrame:
    """
    Check whether each payment recipient exists.

    The recipients_lookup_df must contain validated,
    unique recipient_id_as_int values.
    """
    
    payments_df = payments_df.withColumn(
        "p_recipient_id_as_int",
        F.expr("try_cast(recipient_id as int)")
    )

    return (
        payments_df
        .join(
            recipients_lookup_df.select("recipient_id_as_int"),
            F.col("p_recipient_id_as_int") == F.col("recipient_id_as_int"),
            how="left"
        ).withColumn(
           "existing_recipient",
           F.col("recipient_id_as_int").isNotNull() 
        ).drop("p_recipient_id_as_int", "recipient_id_as_int")
    )


def check_validation(payments_df:DataFrame) -> DataFrame:

    return (
        payments_df
        .withColumn(
            "is_valid",
            F.col("valid_payment_id")
            & F.col("valid_recipient_id")
            & F.col("valid_amount")
            & F.col("valid_date")
            & F.col("valid_status")
            & (F.col("id_occurrences") == 1)
            & F.col("existing_recipient")
        ).withColumn(
            "validation_errors",
            F.concat_ws(
                ", ",
                F.when(~F.col("valid_payment_id"), "Invalid payment_id"),
                F.when(~F.col("valid_recipient_id"), "Invalid recipient_id"),
                F.when(~F.col("valid_amount"), "Invalid amount"),
                F.when(~F.col("valid_date"), "Invalid payment_date"),
                F.when(~F.col("valid_status"), "Invalid status"),
                F.when((F.col("id_occurrences") > 1) & F.col("payment_id").isNotNull(), "Duplicate payment_id"),
                F.when(~F.col("existing_recipient") & F.col("valid_recipient_id"), "Unknown recipient_id")
            )
        ).select(
             "payment_id",
             "recipient_id",
             "amount",
             "payment_date",
             "status",
             "validation_errors",
             "is_valid"
        )
    )
