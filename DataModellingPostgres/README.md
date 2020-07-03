# DataModellingPostgres

## Purpose
* Sparkify music streaming company wants to analyse data they are collecting from their app. As the data resides in JSON files. It is very difficult for the analytical team to analyse that data. so, it is our job to perform data modelling on the data and provide the data in an organized structure for analysis.
## Datasets
* Song Dataset  metadata about the song  
* Log Dataset  user activity log data
## Tools 
* Python and Postgres 
## Database  Schema 
* The schema used for the project is star schema which contains one fact table surrounded by multiple dimension tables. This project consists of one fact fable and four dimension tables
### Fact table  
* Songplays contains all the measures of data
### Dimension tables
* Users table gives information about the users
* Songs table gives information about the songs
* Artists table gives information about the artists
* Time table gives  timestamp of the songs played
## Files created for the project
* sql_queries.py   which contains all the SQL queries for creating, inserting, and dropping commands
* create_table.py  in this file the sql_queries.py file called for creating tables, dropping tables if already exists, inserting the data into the tables
* etl.py  The extraction, transformation, and loading of data is done by running etl.py file which contains code for extracting the data, transforming data to load into the correct tables, and final step as loading the transformed data into the tables in the database.
## ETL steps performed
* Extraction of data from the songs and log files 
* Transforming the extracted data so that the data can fit into the created Postgres tables
* Performing ETL on the data to process and store the data in the respective tables 
## Steps for executing the project
* Step 1 run the creat_tables.py using python
		python creat_tables.py
* Step 2 Run the etl.py to perform the ETL. This etl.py starts processing all the data that is stored in the JSON files and stores the processed data into the database.
		Python etl.py
