# Analysis 10: Determine the Top 5 Vehicle Makes where drivers are charged with speeding related offenses,
# have licensed drivers, use top 10 used vehicle colors, and have cars licensed with the Top 25 states with the
# highest number of offenses.


from pyspark.sql.functions import col, count
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number

def run_analysis10(charges_df, unit_df, endorsements_df):
    """
    Determine the Top 5 Vehicle Makes where drivers are charged with speeding related offences,
    have licensed drivers, used top 10 vehicle colours, and have cars licensed in the Top 25 states with the highest number of offences.

    Args:
        charges_df (DataFrame): DataFrame containing charge information.
        unit_df (DataFrame): DataFrame containing unit information.
        endorsements_df (DataFrame): DataFrame containing endorsements information.

    Returns:
        DataFrame: A DataFrame with the Top 5 Vehicle Makes.
    """
    # Determine the Top 25 states with the highest number of offences
    state_offences_df = charges_df.groupBy("DRVR_LIC_STATE_ID").agg(count("*").alias("offence_count"))
    top_25_states_df = state_offences_df.orderBy(col("offence_count").desc()).limit(25).select("DRVR_LIC_STATE_ID")

    # Determine the Top 10 vehicle colors
    color_counts_df = unit_df.groupBy("VEH_COLOR_ID").agg(count("*").alias("color_count"))
    top_10_colors_df = color_counts_df.orderBy(col("color_count").desc()).limit(10).select("VEH_COLOR_ID")

    # Filter for speeding related offences
    speeding_offences_df = charges_df.filter(col("CHARGE").contains("SPEEDING"))

    # Filter for licensed drivers
    licensed_drivers_df = endorsements_df.filter(col("DRVR_LIC_ENDORS_ID").isNotNull())

    # Join dataframes
    filtered_df = unit_df.join(speeding_offences_df, on=["CRASH_ID", "UNIT_NBR"]) \
                         .join(licensed_drivers_df, on=["CRASH_ID", "UNIT_NBR"]) \
                         .join(top_10_colors_df, on="VEH_COLOR_ID") \
                         .join(top_25_states_df, on="DRVR_LIC_STATE_ID")

    # Count the number of offences per vehicle make
    make_offences_df = filtered_df.groupBy("VEH_MAKE_ID").agg(count("*").alias("offence_count"))

    # Sort by the count of offences and get the Top 5 vehicle makes
    result_df = make_offences_df.orderBy(col("offence_count").desc()).limit(5)

    return result_df
