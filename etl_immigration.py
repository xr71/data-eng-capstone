from pyspark.sql import SparkSession
from datetime import datetime, timedelta
from pyspark.sql import types as t
from pyspark.sql.functions import udf, col, monotonically_increasing_id
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear
import logging

def process_sas_file(spark, input_data, output_path):
    
    df_spark = spark.read.format('com.github.saurfang.sas.spark').load(input_data)
    
    df_spark.show(2, vertical=True)
    
    df_spark.createOrReplaceTempView("stg_immigration_raw")
    
    out_df = spark.sql("""
        select distinct i94yr, i94mon, i94port
        from stg_immigration_raw
        where i94mode = 1 
    """)
    
    out_df.write.mode("overwrite").partitionBy("i94yr", "i94mon").parquet(output_path + "dim_immigration/")