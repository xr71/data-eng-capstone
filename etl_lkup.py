import pandas as pd
from lkup_utils import us_state_codes, city_codes

def process_lookups(spark, output_path):
    """
    Reads the utilities file of code to name mappings into Spark SQL DataFrames and 
        converts them to Parquet
    Input Args: SparkSession object, and 
        the output folder for the Parquet output file
    Output:
        * NA - writes Parquet files out
    """
    
    state_df = pd.DataFrame({"state_code": list(us_state_codes.values()), 
                             "state_name": list(us_state_codes.keys())})
    
    
    city_df = pd.DataFrame({"city_code": list(city_codes.keys()), 
                            "city_name": list(city_codes.values())})
    
    spark_state = spark.createDataFrame(state_df)
    spark_city = spark.createDataFrame(city_df)
    
    spark_state.write.mode("overwrite").parquet(output_path + "lkup_state")
    spark_city.write.mode("overwrite").parquet(output_path + "lkup_city")
    
    print("LKUP DONE")