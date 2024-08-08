# Analysis 4: Determine the number of vehicles with drivers having valid licenses involved in hit and run.

from pyspark.sql.functions import col, countDistinct


def run_analysis4(unit_df, primary_person_df):
    """
    Determine number of Vehicles with driver having valid licences involved in hit and run.

    Args:
        unit_df (DataFrame): DataFrame containing unit information, including vehicle descriptions.
        primary_person_df (DataFrame): DataFrame containing primary person information.

    Returns:
        DataFrame: A DataFrame with a single row containing the count of valid license hit-and-run vehicles.
    """
    # Filter for hit-and-run incidents
    hit_and_run_df = unit_df.filter(col("VEH_HNR_FL") == 'Y')

    # Filter for drivers with valid licenses
    valid_license_df = primary_person_df.filter(col("DRVR_LIC_TYPE_ID") == 'Valid')

    # Join the two DataFrames on CRASH_ID and UNIT_NBR
    joined_df = hit_and_run_df.join(valid_license_df, ["CRASH_ID", "UNIT_NBR"])

    # Count the number of distinct vehicles involved in hit-and-run with valid licenses
    result_df = joined_df.select(countDistinct("UNIT_NBR").alias("ValidLicenseHitAndRunVehicles"))

    return result_df
