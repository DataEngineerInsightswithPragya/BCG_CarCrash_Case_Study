# Analysis 5: Which state has the highest number of accidents in which females are not involved?

from pyspark.sql.functions import limit,count,filter

def run_analysis5(unit_df, primary_person_df):
    """
    Analyze and determine the state with the highest number of accidents in which females are not involved.

    This function joins the `unit_df` and `primary_person_df` DataFrames on `CRASH_ID` and `UNIT_NBR` to filter
    for crashes where no females were involved. It then groups the filtered data by vehicle license state,
    counts the number of accidents per state, and returns the state with the highest count.

    Args:
        unit_df (DataFrame): The DataFrame containing vehicle-related information, including crash ID and vehicle license state.
        primary_person_df (DataFrame): The DataFrame containing information about the persons involved in the crash, including gender.

    Returns:
        DataFrame: A DataFrame with the state (VEH_LIC_STATE_ID) that has the highest number of accidents where females are not involved,
        along with the count of such accidents. The DataFrame will have a single row with columns for `VEH_LIC_STATE_ID` and `count`.
    """
    # Join the dataframes
    joined_df = unit_df.join(
        primary_person_df,
        on=["CRASH_ID", "UNIT_NBR"],
        how="inner"
    )
    # Filter for crashes without female involvement
    state_accidents = joined_df.filter(
        col("PRSN_GNDR_ID") != "Female"
    )
    # Group by state and count
    state_counts = state_accidents.groupBy("VEH_LIC_STATE_ID").count().orderBy(col("count").desc()).limit(1)
    return state_counts

