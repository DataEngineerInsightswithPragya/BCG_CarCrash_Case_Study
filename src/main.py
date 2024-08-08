import yaml
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, min, max, dense_rank, rank, row_number
from analysis.analysis1 import run_analysis1
from analysis.analysis2 import run_analysis2
from analysis.analysis3 import run_analysis3
from analysis.analysis4 import run_analysis4
from analysis.analysis5 import run_analysis5
from analysis.analysis6 import run_analysis6
from analysis.analysis7 import run_analysis7
from analysis.analysis8 import run_analysis8
from analysis.analysis9 import run_analysis9
from analysis.analysis10 import run_analysis10

def load_config(config_path):
    """
    Load YAML configuration file.

    :param config_path: Path to the configuration file.
    :return: Dictionary containing the configuration.
    """
    with open(config_path, 'r') as file:
        return yaml.safe_load(file)

def main():
    """
    Main function to run crash analysis.

    This function performs the following steps:
    1. Creates a Spark session.
    2. Loads input and output configurations from YAML files.
    3. Loads data from CSV files into DataFrames.
    4. Runs multiple analyses using the loaded data.
    5. Saves the results of each analysis to the specified output paths.

    The input and output configurations are expected to be in 'config/input_config.yaml'
    and 'config/output_config.yaml', respectively. The analyses are performed by calling
    separate functions from different analysis modules.
    """
    spark = SparkSession.builder.appName("CrashAnalysis").getOrCreate()

    # Load configuration
    input_config = load_config('config/input_config.yaml')
    output_config = load_config('config/output_config.yaml')

    # Load data
    charges_df = spark.read.csv(input_config['data_sources']['charges'], header=True, inferSchema=True)
    endorsements_df = spark.read.csv(input_config['data_sources']['endorsements'], header=True, inferSchema=True)
    restrict_df = spark.read.csv(input_config['data_sources']['restrict'], header=True, inferSchema=True)
    damages_df = spark.read.csv(input_config['data_sources']['damages'], header=True, inferSchema=True)
    primary_person_df = spark.read.csv(input_config['data_sources']['primary_person'], header=True, inferSchema=True)
    unit_df = spark.read.csv(input_config['data_sources']['unit'], header=True, inferSchema=True)

    # Run analyses
    result1 = run_analysis1(primary_person_df)
    result2 = run_analysis2(unit_df)
    result3 = run_analysis3(primary_person_df, unit_df)
    result4 = run_analysis4(unit_df, primary_person_df)
    result5 = run_analysis5(primary_person_df)
    result6 = run_analysis6(unit_df)
    result7 = run_analysis7(primary_person_df, unit_df)
    result8 = run_analysis8(unit_df, charges_df)
    result9 = run_analysis9(damages_df, unit_df)
    result10 = run_analysis10(charges_df, unit_df, endorsements_df)

    # Save results
    result1.write.csv(output_config['output_paths']['analysis1'], header=True)
    result2.write.csv(output_config['output_paths']['analysis2'], header=True)
    result3.write.csv(output_config['output_paths']['analysis3'], header=True)
    result4.write.csv(output_config['output_paths']['analysis4'], header=True)
    result5.write.csv(output_config['output_paths']['analysis5'], header=True)
    result6.write.csv(output_config['output_paths']['analysis6'], header=True)
    result7.write.csv(output_config['output_paths']['analysis7'], header=True)
    result8.write.csv(output_config['output_paths']['analysis8'], header=True)
    result9.write.csv(output_config['output_paths']['analysis9'], header=True)
    result10.write.csv(output_config['output_paths']['analysis10'], header=True)

if __name__ == "__main__":
    main()
