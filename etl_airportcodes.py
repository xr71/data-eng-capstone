import logging


def process_airports_file(spark, input_data, output_path):
    """
    
    """
    
    df_spark = spark.read.format("csv").option("header", "true").load(input_data)
    
    df_spark.createOrReplaceTempView("stg_airports")

    out_df = spark.sql("""
        select iso_country
               ,iso_region
               ,municipality
               ,ident
               ,iata_code
               ,local_code
               ,type
               ,name
               ,coordinates
               ,SUBSTRING_INDEX(coordinates, ',', -1) as y_coord
               ,SUBSTRING_INDEX(coordinates, ',', 1) as x_coord
               ,elevation_ft
               ,gps_code
        from stg_airports
        where iata_code is not null or local_code is not null
    """)
    
    out_df.write.mode("overwrite").parquet(output_path + "dim_airports/")
    
    print("AIRPORTS DONE")
    