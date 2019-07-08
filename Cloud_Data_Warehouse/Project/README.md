# Project 3: AWS Data Warehouse

--------------------------------------------

## Introduction

This project is for the Data Engineer Nanodegree; creating a data warehouse in AWS using
[S3](https://aws.amazon.com/en/s3/) (Data storage) 
and [Redshift](https://aws.amazon.com/en/redshift/) 

The data sources are JSON files located in two S3 buckets. One bucket contains files pertaining to songs and artists while the other contains data regarding user actions (events). First these files will be imported into staging tables. From which, a proper schema will be produced so that the analysts at 'Sparkify' can properly consumer and analyze the data

--------------------------------------------

## Code and Files:

* __dwh.cfg:__ 
    This file contains the connection information to the Redshift database and the location of the S3 files.

* __sql_queries.py:__ 
    This file contains all the sql statements including drop, create, copy, insert queries.

* __create_tables.py:__ 
    This file contains the code to execute the database connection, dropping, and (re)creating of all neceessary tables

* __etl.py:__ 
    This file contains etl steps to execute in order to create the data schema. 
        Step 1: Load Staging Tables with data from the S3 buckets. 
        Step 2: Insert schema tables with data from stagin tables.

--------------------------------------------


## Schema Set Up

This project will create two staging tables and a star schema database for the final product. The staging tables will be used to stage the S3 bucket data before being transfered to the different tables in the star schema

#### Tables:
* __staging_events:__ Staage data from the log data S3 bucket 
* __staging_songs:__ Staged data from the song data S3 bucket
* __songplays:__ This will be the fact table which will describe the events of songs played with references out to the dimension tables
* __users:__ Holds the data for all the users who have played a song
* __songs:__ Holds the data pertaining to each unique song
* __artists:__ Holds data pertaining to each unique artist
* __time:__ Holds the time dimensional table to describe characterstics of a time of an event in songplays. 


--------------------------------------------

## ETL Process

* __Step 1:__ Set up the AWS Redshifter Cluster database
* __Step 2:__ Update dwh.cfg with database connection information
* __Step 3:__ Run create_tables.py to create all necessary tables
* __Step 4:__ Run etl.py to load data from S3 buckets to tables.


