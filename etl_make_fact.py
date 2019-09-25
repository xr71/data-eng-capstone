import logging
import os
import glob
from functools import reduce
from pyspark.sql import DataFrame

def make_fact_table(spark, output_path):
    """
    Reads all Parquet files from previous ETL steps into a Spark SQL DataFrame that is 
        a fact table made from all of the dimmension tables made  
        for the purpose of easy reporting and analytical queries
    Input Args: SparkSession object, and 
        the output folder for the Parquet output file
    Output:
        * NA - writes a Parquet file out
    """
    
    months_dfs = []

    for month in os.listdir("./output_data/dim_immigration/"):
        tmp = spark.read.parquet(os.path.join("./output_data/dim_immigration/", month))
        months_dfs.append(tmp)
    
    dim_imm = reduce(DataFrame.unionAll, months_dfs)
    demographics = spark.read.parquet("./output_data/dim_demographics/")
    airports = spark.read.parquet("./output_data/dim_airports/")
    temperatures = spark.read.parquet("./output_data/dim_temperatures/")
    lkup_city = spark.read.parquet("./output_data/lkup_city/")
    lkup_state = spark.read.parquet("./output_data/lkup_state/")
    
    dim_imm.createOrReplaceTempView("immigration")
    demographics.createOrReplaceTempView("demographics")
    airports.createOrReplaceTempView("airports")
    temperatures.createOrReplaceTempView("temperatures")
    lkup_city.createOrReplaceTempView("lcity")
    lkup_state.createOrReplaceTempView("lstate")
    
    out_df = spark.sql("""
            select i.*
                  ,lcity.city_name
                  ,lstate.state_name
                  ,d.*
                  ,a.airportcount  
                  ,t.averageTemperature
            from immigration as i
            join lcity on i.destination_city_code = lcity.city_code
            join lstate on i.destination_state_code = lstate.state_code
            left join (
                select state_code
                      ,sum(pop_white_counts) as pop_white_counts
                      ,sum(pop_asian_counts) as pop_asian_counts
                      ,sum(pop_native_american_counts) as pop_native_american_counts
                      ,sum(pop_hispanic_counts) as pop_hispanic_counts
                      ,sum(pop_african_american_counts) as pop_african_american_counts 
                      ,sum(tot_population) as tot_population
                      ,sum(male_population) as male_population
                      ,sum(female_population) as female_population
                      ,sum(veteran_population) as veteran_population
                      ,sum(foreign_population) as foreign_population
                from demographics
                group by state_code
            ) as d on i.destination_state_code = d.state_code
            left join (
                select SUBSTRING_INDEX(iso_region,'-',1) as state
                      ,count(*) as airportcount
                from airports
                group by state
            ) as a
                on i.destination_state_code = a.state
            left join (
                select *
                from temperatures
                where country like '%United%States%'
            ) as t
                on i.year = t.year and i.month = t.month
        """)
    
    print("CHECKING ROWS AVAILABLE IN ALL TABLES")
    spark.sql("""
        select 'immigration' as table_name, count(*) as rowcnt
        from immigration
        
        union all
        select 'demographics' as table_name, count(*) as rowcnt
        from demographics
        
        union all
        select 'airports' as table_name, count(*) as rowcnt
        from airports
        
        union all
        select 'temperatures' as table_name, count(*) as rowcnt
        from temperatures
        
        union all
        select 'lcity' as table_name, count(*) as rowcnt
        from lcity
        
        union all
        select 'lstate' as table_name, count(*) as rowcnt
        from lstate
    """).show()
    
    print("CHECK THAT FACT TABLE HAS SUFFICIENT ROWS")
    print(out_df.count())
    
    out_df.write.mode("overwrite").parquet(output_path + "fct_immigration/")
    
    print("DONE")