# Analysis 7: For all the body styles involved in crashes, mention the top ethnic user group of each
# unique body style.

from pyspark.sql.functions import col, count
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number

def run_analysis7(primary_person_df, unit_df):
    """
    Determine the top ethnic user group for each unique body style involved in crashes.

    Args:
        primary_person_df (DataFrame): DataFrame containing primary person information.
        unit_df (DataFrame): DataFrame containing unit information, including body styles.

    Returns:
        DataFrame: A DataFrame with the top ethnic user group for each unique body style.
    """
    # Join primary person and unit DataFrames on CRASH_ID and UNIT_NBR
    joined_df = primary_person_df.join(unit_df, on=["CRASH_ID", "UNIT_NBR"])

    # Group by body style and ethnic group, and count the number of occurrences
    grouped_df = joined_df.groupBy("VEH_BODY_STYL_ID", "PRSN_ETHNICITY_ID").agg(count("*").alias("count"))

    # Rank the ethnic groups within each body style
    window = Window.partitionBy("VEH_BODY_STYL_ID").orderBy(col("count").desc())
    ranked_df = grouped_df.withColumn("rank", row_number().over(window))

    # Select the top ethnic group for each body style
    result_df = ranked_df.filter(col("rank") == 1).select("VEH_BODY_STYL_ID", "PRSN_ETHNICITY_ID", "count")

    return result_df
