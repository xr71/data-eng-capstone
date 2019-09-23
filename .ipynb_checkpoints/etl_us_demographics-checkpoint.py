import logging


def process_demographics_file(spark, input_data, output_path):
    """
    
    """
    
    df_spark = spark.read.format("csv").option("delimiter", ";").option("header", "true").load(input_data)
    
    df_spark.show(2)
    
    df_spark.createOrReplaceTempView("stg_demographics")

    out_df = spark.sql("""
        select `State Code` as state_code
               ,City as city
               ,State as state
               ,max(
                   case when Race = 'White' then Count else 0 end
               ) as pop_white_counts
               ,max(
                   case when Race = 'Asian' then Count else 0 end
               ) as pop_asian_counts
               ,max(
                   case when Race like 'American%Indian%' then Count else 0 end
               ) as pop_native_american_counts
               ,max(
                   case when Race = 'Hispanic or Latino' then Count else 0 end
               ) as pop_hispanic_counts
               ,max(
                   case when Race like 'Black%African%' then Count else 0 end
               ) as pop_african_american_counts
               ,max(
                   `Total Population` 
               ) as tot_population
               ,max(
                   `Male Population` 
               ) as male_population
               ,max(
                   `Female Population` 
               ) as female_population
               ,max(
                   `Number of Veterans` 
               ) as veteran_population
               ,max(
                   `Foreign-born` 
               ) as foreign_population
               ,max(
                   `Median Age`
               ) as median_age
               ,max(
                   `Average Household Size`
               ) as avg_hh_size
        from stg_demographics
        where `State Code` is not null
        group by `State Code`, city, state
    """)
    
    out_df.write.mode("overwrite").partitionBy("state_code").parquet(output_path + "dim_demographics/")
    
    print("DEMOGRAPHICS DONE")
    