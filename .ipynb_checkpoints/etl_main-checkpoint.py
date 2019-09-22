from pyspark.sql import SparkSession
from datetime import datetime, timedelta
from pyspark.sql import types as t
from pyspark.sql.functions import udf, col, monotonically_increasing_id
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear
import os
import glob
import configparser

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
    
    """

    return glob.glob(os.path.join("/", "data", "18-83510-I94-Data-2016", "*.sas7bdat"))


if __name__ == "__main__":
    sc = create_spark_session()
    
    output_data = "s3a://xuren-data-eng-nd/capstone/"
    
    input_data = input_sas_files()
#     print(input_data)

    df_spark =sc.read.format('com.github.saurfang.sas.spark').load('../../data/18-83510-I94-Data-2016/i94_apr16_sub.sas7bdat')
    
    print(df_spark.show(2, vertical=True))
    
    print("DONE")