# Data Pipelines with Airflow

## Project Introduction
"A music streaming company, Sparkify, has decided that it is time to introduce more automation and monitoring to their data warehouse ETL pipelines and come to the conclusion that the best tool to achieve this is Apache Airflow.[...] The source data resides in S3 and needs to be processed in Sparkify's data warehouse in Amazon Redshift. The source datasets consist of JSON logs that tell about user activity in the application and JSON metadata about the songs the users listen to."

## Prerequisites

All tables must be created, and running in Redshift before executing the DAG workflow. The create tables statements can be found in:

`create_tables.sql`

## Data Sources

Data resides in two directories that contain files in JSON format:

1. Log data: s3://udacity-dend/log_data
2. Song data: s3://udacity-dend/song_data


## Python Scripts 

* `create_tables.sql` - Contains the SQL statements for all necessary tables 
* `udac_example_dag.py` - The DAG configuration file 
* `stage_redshift.py` - Operator to stage data in Redshift
* `load_fact.py` - Operator to load the fact table
* `load_dimension.py` - Operator to load the dimension tables 
* `data_quality.py` - Operator for data quality checking

