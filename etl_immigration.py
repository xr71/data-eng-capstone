import logging

def process_sas_file(spark, input_data, output_path):
    """
    Reads the SAS files for i94 immigration month by month 
        into a Spark SQL DataFrame and converts it to Parquet after cleaning it
    Input Args: SparkSession object, the input raw file, and 
        the output folder for the Parquet output file
    Output:
        * NA - writes a Parquet file out
    """
    
    df_spark = spark.read.format('com.github.saurfang.sas.spark').load(input_data)
    
#     df_spark.show(2, vertical=True)
    month = input_data.split("_")[1]
    
    df_spark.createOrReplaceTempView("stg_immigration_raw")
    
    out_df = spark.sql("""
        select i94yr as year
              ,i94mon as month
              ,i94bir as age
              ,i94res as origin_country_code
              ,i94addr as destination_state_code
              ,i94port as destination_city_code
              ,gender
              ,i94visa as visa_code
              ,visatype
              ,sum(count) as i94_counts
        from stg_immigration_raw
        where i94mode = 1
            and i94port is not null
        group by year, month, age,
                 origin_country_code,
                 destination_state_code,
                 destination_city_code,
                 gender, visa_code,
                 visatype
    """)
    
    out_df.write.mode("overwrite").partitionBy("year", "month").parquet(output_path + "dim_immigration/"+month+"/")