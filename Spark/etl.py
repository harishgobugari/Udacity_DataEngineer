import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.functions import col, year, month, dayofmonth, hour, weekofyear, date_format, monotonically_increasing_id


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    # get filepath to song data file
    song_data = input_data + 'song_data/*/*/*/*.json'
    
    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = df.select(['title', 'artist_id', 'year', 'duration']).withColumn("song_id",monotonically_increasing_id())
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.parquet(output_data+'songs',partitionBy=('year','artist_id'))

    # extract columns to create artists table
    artists_table =  df.select('artist_id',
                              col('artist_name').alias("name"),
                              col('artist_location').alias('location'), 
                              col('artist_latitude').alias('latitude'), 
                              col('artist_longitude').alias('longitude')).distinct()
    
    # write artists table to parquet files
    artists_table..write.parquet(output_data+'artists')


def process_log_data(spark, input_data, output_data):
    # get filepath to log data file
    log_data =input_data+'log_data/*/*/*.json'

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df =  df.filter(col("page") == 'NextSong')

    # extract columns for users table    
    users_table = df.select(col('userId').alias('user_id'),
                            col('firstName').alias('first_name'),
                            col('lastName').alias('last_name'),'gender', 'level').distinct()
    
    # write users table to parquet files
    users_table.write.parquet(output_data+'users')

    # create timestamp column from original timestamp column
    df = df.withColumn("timestamp", F.to_timestamp(df.ts/1000))

    # create datetime column from original timestamp column
    df = df.withColumn("datetime", F.to_date(df.timestamp))
    
    # extract columns to create time table
    time_table = df.selectExpr(["timestamp as start_time",
                                "hour(datetime) as hour",
                                "dayofmonth(datetime) as day",
                                "weekofyear(datetime) as week",
                                "month(datetime) as month",
                                "year(datetime) as year",
                                "dayofweek(datetime) as weekday"]).dropDuplicates()
    
    # write time table to parquet files partitioned by year and month
    time_table.write.parquet(output_data+'time',partitionBy=('year','month'))

    # read in song data to use for songplays table
    songs = spark.read.parquet(output_data+'songs/*/*')
    artist = spark.read.parquet(output_data+'artists/*')

    songs = songs.select('song_id','title')
    songs_df = songs.join(df,songs.title == df.song, 'inner')
    artist = artist.select('artist_id','name')
    artist_log = songs_df.join(artist, (songs_df.artist == artist.name), how = 'inner')
    artist_log = artist_log.select('userId', 'level', 'song_id', 'artist_id', 'sessionId', 'location', 'userAgent','ts')
    songsplay = artist_log.join(time_table,(F.to_timestamp(artist_log.ts/1000)) == time_table.start_time, 'inner')

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = songsplay.select('start_time', 
                                        'userId', 
                                        'level', 
                                        'song_id', 
                                        'artist_id', 
                                        'sessionId', 
                                        'location', 
                                        'userAgent', 
                                        'year', 
                                         'month')

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.parquet(output_data+'songplays', partitionBy=('year','month'))


def main():
    spark = create_spark_session()
    input_data = "s3n://udacity-dend/"
    output_data = "s3n://sparkudacity/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
