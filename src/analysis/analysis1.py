# Analytics 1: Find the number of crashes (accidents) in which the number of males killed is greater than 2.

from pyspark.sql.functions import col,count


def run_analysis1(primary_person_df):
    """
    Analyzes crashes where the number of males killed is greater than 2.

    This function filters the `primary_person_df` DataFrame to include only those records where:
    - The gender of the person is 'Male'
    - The count of deaths (`DEATH_CNT`) is greater than 2

    It then counts the number of distinct `CRASH_ID`s from these filtered records.

    Args:
        primary_person_df (DataFrame): DataFrame containing primary person information involved in crashes.

    Returns:
        DataFrame: A DataFrame with a single row containing the count of distinct `CRASH_ID`s where
                   more than 2 males were killed.
    """
    # Filter crashes with more than 2 males killed
    crashes_with_males = primary_person_df.filter(
        (col("PRSN_GNDR_ID") == "Male") &
        (col("DEATH_CNT") > 2)
    )

    # Count distinct crash IDs
    count = crashes_with_males.select("CRASH_ID").distinct().count()

    # Return result as DataFrame
    result_df = crashes_with_males.select(col("CRASH_ID")).distinct().groupBy().count()
    return result_df
