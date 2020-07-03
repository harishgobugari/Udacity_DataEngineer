# Datawarehouse

## Introduction
* Sparkify, a music streaming company wants to move data and processes into the cloud because of their growth The data of the sparkify currently stored in JSON files. As data is growing huge, it becomes very difficult for the sparkify to process and analyze. So sparkify found a way to solve this problem using AWS and redshift 

## Project Details
* For this project, we are moving data into S3 buckets
### S3 Buckets (Data Storage):
* Bucket 1 contains metadata about songs  
SONG_DATA='s3://udacity-dend/song_data' 
* Bucket 2 contains user activity log data  
LOG_DATA='s3://udacity-dend/log_data'  

We are loading data from s3 bucket into redshift which is columnar storage  Dataware house.

The schema used for the project is star schema which contains one fact table surrounded by multiple dimension tables. This project consists of one fact fable and four dimension tables.
## Fact table  
* Songplays contains all the measures of data
## Dimension tables
* Users table gives information about the users
* Songs table gives information about the songs
* Artists table gives information about the songs
* Time table gives  timestamp of the songs played
## Run process
* Start the AWS Redshift Cluster
*  Run create_tables.py to create tables
*  Run etl.py to perform extraction, transformation, and loading
## Files used
* create_tables.py - it will create tables
* etl.py - it will extract JSON data from the S3 bucket and ingest them to Redshift
* sql_queries.py - contains all SQL queries for creating, inserting, and dropping
* dhw.cfg - consists of information about Redshift, IAM and S3

