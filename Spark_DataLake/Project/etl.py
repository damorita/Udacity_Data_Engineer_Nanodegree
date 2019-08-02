import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType, TimestampType
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
import pyspark.sql.functions as F


config = configparser.ConfigParser()
# config.read('dl.cfg')
config.read_file(open('dl.cfg'))

#os.environ['AWS_ACCESS_KEY_ID']=config['AWS_ACCESS_KEY_ID']
#os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS_SECRET_ACCESS_KEY']

def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark



def process_song_data(spark, input_data, output_data):
    
    """Load JSON song_data from input_data,
        process the data to extract song_table and artists_table, and
        store the queried data to parquet files.
    Keywords:
    * spark         -- reference to Spark session.
    * input_data    -- path to input_data to be processed (song_data)
    * output_data   -- path to location to store the output (parquet files)."""
    
    # get filepath to song data file
    #song_data = input_data + "song-data/*/*/*/*.json"
    song_data = input_data
    print(song_data)
    
    # read song data file
    print("Reading song_data files from {}...".format(song_data))
    #df = spark.read.json(song_data, schema=songdata_schema)
    df = spark.read.format("json").load(song_data)
    df.printSchema()
    print("Finished reading song_data files from {}...".format(song_data))
    

    # extract columns to create songs table
    print("Extracting columns to create song_table ... ")
    songs_table = df.select('song_id', 'artist_id', 'year', 'duration')
    print("Done")
    
    # write songs table to parquet files partitioned by year and artist
    print("Writing to parquet files partitioned by year & artist ... ")
    songs_table.write.mode('overwrite').mode('overwrite').partitionBy('year', 'artist_id').parquet(output_data + "songs")
    print("Done")

    # extract columns to create artists table
    print("Extracting columns to create artists_table ... ")
    artists_table = df.select('artist_id', 'artist_name', 'artist_location', 'artist_latitude','artist_longitude')
    print("Done")
    
    # write artists table to parquet files
    print("Writing to parquet files ... ")
    artists_table.write.mode('overwrite').mode('overwrite').parquet(output_data + "artists")
    print("Done")




def process_log_data(spark, input_data, output_data):
    """ Load JSON log_data from input_data,
        process the data to extract users, time and songplay tables, and
        store the queried data to parquet files. 
    Keywords:
    * spark         -- reference to Spark session.
    * input_data    -- path to input_data to be processed (song_data)
    * output_data   -- path to location to store the output (parquet files)."""

    # get filepath to log data file
    log_data = input_data

    # read log data file
    print("Reading Log Data files ... ")
    #df = spark.read.json(log_data, schema = logdata_schema)
    df = spark.read.json(log_data)
    df.printSchema()
    print("Done")
    
    # filter by actions for song plays
    df = df.filter(col("page") == 'NextSong')

    # extract columns for users table    
    print("Extracting Columns for User Table ... ")
    users_table = df.select("userId","firstName", "lastname","gender","level") 
    print("Done")

    
    # write users table to parquet files
    print("Writing Users table to parquet files")
    users_table.write.mode('overwrite').parquet(output_data+"users")
    print("Done")

    # create timestamp column from original timestamp column
    print("Create timestamp column")
    get_timestamp = udf(lambda x: datetime.fromtimestamp(x / 1000), TimestampType())
    df = df.withColumn('timestamp', get_timestamp(df.ts))
    print("Done")
    
    # create datetime column from original timestamp column
    print("Create datetime columns")
    get_datetime = udf(lambda x: F.to_date(x), TimestampType())
    df = df.withColumn('datetime', get_datetime(df.ts))
    print("Done")
    
    # extract columns to create time table
    print("Create time_table")
    
    time_table = df.select(col("timestamp").alias("start_time"),
                                   hour(col("timestamp")).alias("hour"),
                                   dayofmonth(col("timestamp")).alias("day"), 
                                   weekofyear(col("timestamp")).alias("week"), 
                                   month(col("timestamp")).alias("month"),
                                   year(col("timestamp")).alias("year"))
    print("Done")
    
    # write time table to parquet files partitioned by year and month
    print("Time table to Parquet Files")
    #time_table.write.mode('overwrite').partitionBy("year","month").parquet(output_data+"time")
    time_table.write.mode('overwrite').partitionBy("year", "month").parquet(output_data + "time_table")
    print("Done")

    # read in song data to use for songplays table
    print("Read Song Data")
    #song_data = config['LOCAL']['INPUT_DATA_SD_LOCAL']
    song_data = config['AWS']['INPUT_DATA_SD']
    song_df = spark.read.json(song_data)
    print("Done")

    # extract columns from joined song and log datasets to create songplays table 
    #create temp tables
    print("Creating temp tables")
    log_df = df
    log_df.createOrReplaceTempView("log_df_table")
    song_df.createOrReplaceTempView("song_df_table")
    time_table.createOrReplaceTempView("time_table")
    print("Done")
    
    print("Create Songplays Table")
    songplays_table = spark.sql(
        """
        SELECT log_df_table.timestamp, 
            log_df_table.userId, 
            log_df_table.level, 
            log_df_table.sessionId, 
            log_df_table.location,
            log_df_table.userAgent, 
            song_df_table.song_id, 
            song_df_table.artist_id,
            time_table.month,
            time_table.year
        FROM log_df_table 
        INNER JOIN song_df_table 
        ON song_df_table.artist_name = log_df_table.artist 
        INNER JOIN time_table
        ON time_table.start_time = log_df_table.timestamp
        """)
    #        GROUP BY time_table.year, time_table.month

    print("Done")

    # write songplays table to parquet files partitioned by year and month
    print("Create Songplays parquet files")
    songplays_table.write.mode('overwrite').partitionBy("year","month").parquet(output_data+"songplays")
    print("Done")

def main():
    spark = create_spark_session()
    
    #output_data = config['LOCAL']['OUTPUT_DATA_LOCAL']
    output_data = config['AWS']['OUTPUT_DATA']

    # Process Song Data
    #input_data = config['LOCAL']['INPUT_DATA_SD_LOCAL']
    input_data = config['AWS']['INPUT_DATA_SD']
    process_song_data(spark, input_data, output_data)  
        
    #Process Log Data
    #input_data = config['LOCAL']['INPUT_DATA_LD_LOCAL']
    input_data = config['AWS']['INPUT_DATA_LD']
    process_log_data(spark, input_data, output_data)

if __name__ == "__main__":
    main()
