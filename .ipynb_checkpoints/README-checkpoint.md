# Data Engineering Capstone for Udacity - Spark ETL 

## Objective
This is the final project for the Udacity Data Engineer Nanodegree. In this project, our objective is to turn large SAS datasets into Parquet files and blend the data with additional sources of data such as airports, demographics, and temperatures. We will be using Spark in this project in order to ETL the data into a star schema that ultimately creates a fact table that is ideal for analytical and reporting needs for our users. 

## About the Data
The primary dataset is from the US National Tourism and Trade Office. It is for every month in the year 2016 and it is stored in SAS dataset binary formats. In this repository, there is a sample of this dataset in the file `immigration_data_sample.csv`. As you will notice in these data tables, there can be a lot of missing values. One of the things we will be doing in this project is to clean the data to the key columns. Furthermore, the datasets are quite large, with all 12 months being nearly 7 GBs of data. One of the decisions we will be making is how to preserve sufficient analytical information about the visas and immigration entries while reducing the number of rows and columns in the dataset. 

## Purpose of the Data
The schema I have designed is intended for use by downstream analytics teams; for example, analysts who need to create higher level reporting around aggregated metrics such as number of entries per state or the level of demographic diversity by state or the number of airports by various dimensions. Attention has been paid particularly to maintaining as much information as possible about the various attributes of the larger monthly immigration dataset such that analysts can slice and dice the data by gender, age, month of entry, visa type of entry, etc. 

This is so that the analysts or reporting teams using the final fact table do not have to perform excess number of joins and thus degrade business intelligence or slice-and-dice performance. 

## Technologies Used and Decisions Made
The primary technology used here is Apache Spark through the Pyspark API. We leverage Spark SQL greatly so that we can prototype as well as maintain and enhance the ETL pipelines rapidly moving forward. 

More importantly, we use Pyspark as a way to scale performance for the future as well, since the datasets may become even larger over time, and performing these ETL steps in memory using Pandas is simply not scalable. 

One of the first things we notice about the raw data is the high number of columns and the inconsistent number of null values in the various columns. Because I am focused primarily on designing an easy to use aggregated reporting table for my downstream users, I took attention to remove columns from the raw immigration i94 tables that do not provide dimensional value for my users. In this case, I decided to keep the aggregations at the year, month, age, gender, origin country, destination state, destination city, visa code, and visa type level while aggregating the total counts of i94 records, given that i94mode is 1 and i94port is not null. This step is important because I am making a conscious design choice for my users that I would otherwise have carefully scoped out and discussed with the team. I am limiting the dataset to i94mode=1 because this represents all Air based entries, as this is what my end users are interested in analyzing or reporting, and I ensure that i94port is not missing such that there are valid US cities to report on. In a project setting, we may further discuss with the team whether we want to remove any rows where a i94port code is not available, but this will be again discussed with all relevant stakeholders and iterated upon.  


## Final Data Model

My final data model contains 2 lookup tables, 4 dimmensions tables, and a fact table that is pre-joined for ease of use for BI or analytics. 

`lkup_city`
```
-- city_code 
-- city_name
```

`lkup_state`
```
-- state_code
-- state_name
```

`dim_airports`
```
-- iso_country
-- iso_region
-- municipality
-- ident
-- iata_code
-- local_code
-- type
-- name
-- coordinates
-- y_coord
-- x_coord
-- elevation_ft
-- gps_code
```

`dim_demographics`
```
-- city
-- state
-- pop_white_counts
-- pop_asian_counts
-- pop_native_american_counts
-- pop_hispanic_counts
-- pop_african_american_counts
-- tot_population
-- male_population
-- female_population
-- veteran_population
-- foreign_population
-- median_age
-- avg_hh_size
-- state_code
```

`dim_immigration`
```
-- age
-- origin_country_code
-- destination_state_code
-- destination_city_code
-- gender
-- visa_code
-- visatype
-- i94_counts
-- year
-- month
```

`dim_temperatures`
```
-- fulldate
-- month
-- day
-- country
-- city
-- averageTemperature
-- latitude
-- longitude
-- year
```

`fct_immigration`
```
 -- age                         
 -- origin_country_code         
 -- destination_state_code      
 -- destination_city_code       
 -- gender                      
 -- visa_code                   
 -- visatype                    
 -- i94_counts                  
 -- year                        
 -- month                       
 -- city_name                   
 -- state_name                  
 -- state_code                  
 -- pop_white_counts            
 -- pop_asian_counts            
 -- pop_native_american_counts  
 -- pop_hispanic_counts         
 -- pop_african_american_counts 
 -- tot_population              
 -- male_population             
 -- female_population           
 -- veteran_population          
 -- foreign_population          
 -- airportcount                
 -- averageTemperature       
```

## Instruction

To run this project, you will need access to a local Spark instances as well as access to the raw data. Since we do not have immediate access to the raw data, you may need to reach out to Udacity for the full raw datasets. 

As an alternative, you are free to read through the Jupyter Notebooks first to get a sense of what's available in the datasets.  

If you are able to run the ETL pipeline by having access to the raw datasets, simply run `python etl_main.py`, assuming that you have your Spark instance configured correctly.

If you are not interested in running the entire pipeline, the Parquet data files have been provided to you here in the `output_data` folder. Simply create a Jupyter Notebook and read the `fct_immigration` Parquet director into Spark and perform your analytics. 

## Analytical Example
An example of analytics query is provided for you along with a `matplotlib` plot of number of i94 entries by age in the `demo_fact_analytics.ipynb` file.


## Additional Scenarios for Consideration

#### Data Increased by 100x:
Currently, the data set is large enough that we couldn't simply read it into memory using something like `Pandas` but it also wasn't so large that we needed a distributed Hadoop environment. However, if the datasets increased to over 700 GB (a factor of 100x), I would migrate the storage to a BLOB environment such as Amazon S3 and spin up clustered nodes of EMR as needed in order to process the massive data files. 

#### The pipelines would be run on a daily basis by 7 am every day:
I would highly suggest a high availability managed service of Airflow to satisfy this requirement. For this, I would either deploy my own Airflow cluster in something like AWS EC2 or use Google Cloud's Cloud Composer to orchestrate this scheduling. Cloud Composer is a fully managed instance of Airflow and can run in the background and is scalable, so we can add resources each morning and use the Airflow scheduling `@daily` to orchestrate this. 

#### The database needed to be accessed by 100+ people:
Because we can grant read-only user access to over 100 people, we would most likely want to use some form of scalable warehouse service, such as Amazon Redshift. We can also use other vertical columnar and highly scalable services such as Azure SQL DW, Snowflake, or Google BigQuery. 
