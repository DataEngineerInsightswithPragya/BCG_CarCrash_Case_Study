# Analysis 8: Among the crashed cars, what are the Top 5 Zip Codes with the highest number of crashes with alcohol
# as a contributing factor? (Use Driver Zip Code)

from pyspark.sql.functions import col, count

def run_analysis8(unit_df, charges_df):
    """
    Determine the Top 5 Zip Codes with the highest number of crashes where alcohol is a contributing factor.

    Args:
        unit_df (DataFrame): DataFrame containing unit information.
        charges_df (DataFrame): DataFrame containing charge information.

    Returns:
        DataFrame: A DataFrame with the Top 5 Zip Codes and their crash counts.
    """
    # Filter charges to get those involving alcohol
    alcohol_crashes_df = charges_df.filter(col("CHARGE").contains("ALCOHOL"))

    # Join with unit data to get driver zip codes
    joined_df = alcohol_crashes_df.join(unit_df, on=["CRASH_ID", "UNIT_NBR"])

    # Count the number of crashes per zip code
    zip_code_crashes_df = joined_df.groupBy("DRVR_ZIP").agg(count("*").alias("crash_count"))

    # Sort by crash count and get the Top 5 zip codes
    result_df = zip_code_crashes_df.orderBy(col("crash_count").desc()).limit(5)

    return result_df
