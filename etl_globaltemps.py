import logging


def process_temp_file(spark, input_data, output_path):
    """
    
    """
    
    df_spark = spark.read.format("csv").option("header", "true").load(input_data)
    
    df_spark.createOrReplaceTempView("stg_temperatures")

    out_df = spark.sql("""
        select date(dt) as fulldate
               ,year(date(dt)) as year
               ,month(date(dt)) as month
               ,day(date(dt)) as day
               ,country
               ,city
               ,averageTemperature
               ,latitude
               ,longitude
        from stg_temperatures
        where averageTemperature is not null
            and year(date(dt)) >= 1900
    """)
    
    out_df.write.mode("overwrite").partitionBy("year").parquet(output_path + "dim_temperatures/")
    
    print("TEMPERATURES DONE")
    