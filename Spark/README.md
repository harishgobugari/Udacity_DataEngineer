# Spark

## Introduction
* Sparkify, a music streaming company wants to move data and processes into the cloud because of their growth The data of the sparkify currently stored in JSON files in data lake hosted in aws s3. 
## Project Details
* For this project, the data is stored in datalake hosted in s3
### S3 (Data lake):
* SONG_DATA='s3://udacity-dend/song_data'  contains metadata about songs
* LOG_DATA='s3://udacity-dend/log_data' contains user activity log data

We are loading data from data lake into spark dataframes.
The schema used for the project is star schema which contains one fact table surrounded by multiple dimension tables. This project consists of one fact fable and four dimension tables.

## Fact table  
* Songplays contains all the measures of data
## Dimension tables
* Users table gives information about the users
* Songs table gives information about the songs
* Artists table gives information about the songs
* Time table gives  timestamp of the songs played
## Run process
Run etl.py to perform extraction, transformation, and loading  
The etl process starts after running etl.py which performs 

* loading data from datalake in s3
* transforming data
* writing data into s3 in parquet fromat

## Files used
* etl.py - it will extract JSON data from the data lake  hosted  in S3 and process data using sprak emr cluster 
* dl.cfg - consists of information about aws aceess keys
