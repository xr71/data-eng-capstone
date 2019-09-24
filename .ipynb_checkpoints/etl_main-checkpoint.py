from pyspark.sql import SparkSession
from datetime import datetime, timedelta
from pyspark.sql import types as t
from pyspark.sql.functions import udf, col, monotonically_increasing_id
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear
import os
import glob
import configparser
import logging

from etl_immigration import process_sas_file 
from etl_us_demographics import process_demographics_file
from etl_airportcodes import process_airports_file
from etl_globaltemps import process_temp_file
from etl_lkup import process_lookups
from etl_make_fact import make_fact_table

def create_spark_session():
    """
    Creates an Apache Spark Session to process the data
    Input Args: None
    Output:
    * spark -- An Apache Spark Session
    """
    
    print("Preparing Spark session for the pipeline...")
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "saurfang:spark-sas7bdat:2.0.0-s_2.11") \
        .enableHiveSupport().getOrCreate()
    
    print("Spark session preparation DONE.")

    return spark


def input_sas_files():
    """
    Lists all of the sas raw binary tables that need to be processed as directory paths
    Input Args: None
    Output: a glob list of directory paths to each SAS7BDAT file
    """

    return glob.glob(os.path.join("/", "data", "18-83510-I94-Data-2016", "*.sas7bdat"))


if __name__ == "__main__":
    sc = create_spark_session()
    
    output_path = "./output_data/"
    
    # process all raw i94 SAS files into monthly parquet files for entire year of 2016
#     input_data = input_sas_files()
#     for fname in input_data:
#         print(fname)
#         process_sas_file(sc, fname, output_path)
    
    # process us demographics file and write to parquet files
#     process_demographics_file(sc, "us-cities-demographics.csv", output_path)
    
    # process airports file
#     process_airports_file(sc, "airport-codes_csv.csv", output_path)
    
    # process global temperatures file
#     process_temp_file(sc, "/data2/GlobalLandTemperaturesByCity.csv", output_path)
    
    # create lookup tables
#     process_lookups(sc, output_path)


    #####################################################################################
    
    make_fact_table(sc, output_path)
