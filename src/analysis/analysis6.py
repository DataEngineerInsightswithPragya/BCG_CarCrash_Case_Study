# Analysis 6: Which are the Top 3rd to 5th VEH_MAKE_IDs that contribute to the largest number of injuries including
# death?

from pyspark.sql.functions import col, sum
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number


def run_analysis6(unit_df):
    """
    Determine the Top 3rd to 5th VEH_MAKE_IDs that contribute to the largest number of injuries including death.

    Args:
        unit_df (DataFrame): DataFrame containing unit information, including vehicle descriptions.

    Returns:
        DataFrame: A DataFrame with the 3rd to 5th VEH_MAKE_IDs and their total injury count.
    """
    # Aggregate total injuries including death by VEH_MAKE_ID
    injuries_df = unit_df.groupBy("VEH_MAKE_ID").agg(
        sum("TOT_INJRY_CNT").alias("total_injuries"),
        sum("DEATH_CNT").alias("total_deaths")
    ).withColumn(
        "total_harm", col("total_injuries") + col("total_deaths")
    )

    # Sort by total harm and rank
    window = Window.orderBy(col("total_harm").desc())
    ranked_df = injuries_df.withColumn("rank", row_number().over(window))

    # Filter to get the 3rd to 5th ranked VEH_MAKE_IDs
    result_df = ranked_df.filter((col("rank") >= 3) & (col("rank") <= 5)).select("VEH_MAKE_ID", "total_harm")

    return result_df
