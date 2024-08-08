# Analysis 9: Count of distinct Crash IDs where no damaged property was observed and Damage Level is above 4 and car avails
# Insurance.

from pyspark.sql.functions import col, countDistinct

def run_analysis9(damages_df, unit_df):
    """
    Count distinct Crash IDs where no damaged property was observed, damage level is above 4, and car avails insurance.

    Args:
        damages_df (DataFrame): DataFrame containing damage information.
        unit_df (DataFrame): DataFrame containing unit information.

    Returns:
        DataFrame: A DataFrame with the count of distinct Crash IDs.
    """
    # Filter for records with no damaged property
    no_damages_df = damages_df.filter(col("DAMAGED_PROPERTY").isNull())

    # Filter for damage level above 4
    high_damage_df = unit_df.filter(col("VEH_DMAG_SCL_1_ID") > 4)

    # Filter for cars with insurance proof
    insured_cars_df = unit_df.filter(col("FIN_RESP_PROOF_ID").isNotNull())

    # Join the dataframes
    filtered_df = no_damages_df.join(high_damage_df, on="CRASH_ID").join(insured_cars_df, on=["CRASH_ID", "UNIT_NBR"])

    # Count distinct Crash IDs
    result_df = filtered_df.select("CRASH_ID").distinct().count()

    return result_df
