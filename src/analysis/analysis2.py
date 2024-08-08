from pyspark.sql.functions import col


def run_analysis2(unit_df):
    """
    Analyzes the given DataFrame to count the number of two-wheelers involved in crashes.

    This function performs the following steps:
    1. Filters the `unit_df` DataFrame to retain only rows where the vehicle description ID (`UNIT_DESC_ID`) is "Two-Wheeler".
    2. Counts the number of such filtered rows.
    3. Returns a DataFrame containing the count of two-wheelers.

    Args:
        unit_df (DataFrame): DataFrame containing unit information, including vehicle descriptions.

    Returns:
        DataFrame: A DataFrame with a single row containing the count of two-wheelers.
    """
    # Filter and count two wheelers booked for crashes
    two_wheelers_df = unit_df.filter(col("UNIT_DESC_ID") == "Two-Wheeler")
    count = two_wheelers_df.count()

    # Return result as DataFrame
    result_df = two_wheelers_df.groupBy().count()
    return result_df
