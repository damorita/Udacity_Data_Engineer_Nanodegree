# Project 1: Data Modeling in Postgress

---

## Summary
This the first project in Udacity's Data Engineer Nanodegree: Creating a Data Model in Postgress with Python.
The etl script contains the following steps to create the data model:
1) Connect and Create the Song Inventory Database and Tables
2) Preprocess the song and log data files to be inserted into a relational database
3) Calculate New (DateTime) columns derived from existing data
4) Insert Data into Tables

---

## Files

+ **create_tables.py:** 
    + Contains custom functions to create tables in Postgress Database
+ **etl.ipynb:** 
    + Contains the jupyter notebook to guide the step by step process to create the data model in postgress
+ **etl.py:**
    + Python file that creates the data abse tables, reads data from files, inserts data into tables
+ **sql_queries:**
    + Contains the SQL Queries to Create, Drop, Insert and Select desired database tables and data.
+ **test.ipynb:** 
    + Contains test cases to run to determine if ETL scripts succeded

---

## Folder Structure
+ /data
    + Contains data files for Song and log data.

---

## Data Model 

### Tables
1) Songs Table
2) Artist Table
3) Time Table
4) Users Table
5) Songplay Table

### Structure
The model is a star schema with the Songs table being the main fact table with the others being dimension tables that additoonal information to expand information about the songs.

### Other Notes
The Model is still yet to be fully cleaned and optimized. For example there are duplications in the Users table. This occurs due to the users playing multiple songs hence their names appear multiple times when represented in a tabular format. The rows should be filtered to unique users before inserted into the table.