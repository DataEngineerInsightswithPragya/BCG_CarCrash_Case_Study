from pyspark.sql import DataFrame
from pyspark.sql.functions import col, count

def run_analysis3(primary_person_df,unit_df):
    """
    Determines the top 5 vehicle makes involved in crashes where the driver died and the airbags did not deploy.

    This function performs the following steps:
    1. Filters the primary_person_df DataFrame to include only records where the driver died and airbags did not deploy.
    2. Joins this filtered data with the unit_df DataFrame to get vehicle make information.
    3. Counts the occurrences of each vehicle make.
    4. Sorts the counts in descending order and selects the top 5 vehicle makes.

    Args:
        primary_person_df (DataFrame): DataFrame containing primary person information.
        unit_df (DataFrame): DataFrame containing unit information, including vehicle makes.

    Returns:
        DataFrame: A DataFrame containing the top 5 vehicle makes based on their occurrence in the specified crashes.
    """
    # Filter primary_person_df for cases where the driver died and airbags did not deploy
    fatal_crashes_df = primary_person_df.filter(
        (col("DEATH_CNT") > 0) & (col("PRSN_AIRBAG_ID") == "No")
    )

    # Join with unit_df to get the vehicle make information
    # Ensure that the join condition matches the column names used for identifying units
    crashes_with_makes_df = fatal_crashes_df.join(
        unit_df, on=["CRASH_ID", "UNIT_NBR"], how="inner"
    )

    # Count the occurrences of each vehicle make
    make_counts_df = crashes_with_makes_df.groupBy("VEH_MAKE_ID").agg(count("VEH_MAKE_ID").alias("count"))

    # Sort by count in descending order and select the top 5
    top5_makes_df = make_counts_df.orderBy(col("count").desc()).limit(5)

    return top5_makes_df
