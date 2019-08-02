# Project 4: Apache Spark & Data Lake

## Summary
This is the 4th project within the Udacity Data Engineer Nanodegree.
The project utilizes pySpark to develop an ETL process to build out star schema data model in Spark.
The output of the files are partitioned parquet file deployed locally and/or in a AWS S3 ready for cosumption.

--------------------------------------------

#### Data Source
The data source is located in Udacity's public S3 buckets: *s3a://udacity-dend/song_data*, and *s3a://udacity-dend/log_data*. These buckets contain JSON files regarding songs, artists and log files of by users respectively for a fake music streaming company called Sparkify

--------------------------------------------
#### Spark process

 - *dl.cfg:* This file contains the configuration information to connect to the appropriate S3 buckets as well as data files stored locally
  - *etl.ipynb:* This file contains python code used to test and write the content in etl.py
  - *etl.py:* This file contains python code used to transform the data from S3 buckets, and deploy the data into partitioned files for Spark.

-------------------------
#### Implementation
1. Update dl.cfg with data source location and access configurations for etl 
2. If needed, update data source location and output destination in etl.py file
3. Run python etl.py in terminal or workspace