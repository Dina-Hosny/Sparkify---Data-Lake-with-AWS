import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format, monotonically_increasing_id
from pyspark.sql.types import TimestampType, DateType



config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS_SECRET_ACCESS_KEY']

SOURCE_S3_BUCKET = config['SOURCE_S3_BUCKET']
DEST_S3_BUCKET = config['DEST_S3_BUCKET']


def create_spark_session():
    
    """
    Creates a spark session instance object

    Arguments:
    None

    Returns:
    A spark session instance object
    """
    
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark




def process_song_data(spark, input_data, output_data):
    
    """
    Reads the song_data JSON, and creates the songs and artists tables per specs. the tables are then
    written into the S3 bucket as parquet files.

    Arguments:
    spark:      The current spark session being used
    input_data: Directory path of where raw song_data exists. In this case it will be an S3 bucket
    input_data: Directory path of where processed data will be stored. In this case it will be an 
               S3 bucket
           
    Returns:
    None
    """
    
    
    # get filepath to song data file
    ####input_data = "s3a://udacity-dend/"
    
    # get filepath to song data file
    song_data = input_data + "song_data/*/*/*/*.json"
    
    
    
    log_data = os.path.join(input_data + 'log_data/*/*/*.json')
    
    # read song data file
    df = spark.read.json(song_data)
    
    # extract columns to create songs table
    songs_table = df.select('song_id', 'title', 'artist_id','year','duration').distinct()
    
    # create temp view of songs table (with additonal col artist_name) to use later when creating songplays table in process_log_data()
    df.select('song_id', 'title', 'artist_id', 'year', 'duration', 'artist_name').distinct().createOrReplaceTempView('songs_table_view')
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.mode('overwrite').partitionBy('year','artist_id').parquet(path=output_data + 'songs')
    
    
    # extract columns to create artists table
    artists_table = df.select('artist_id', 'artist_name', 'artist_location','artist_latitude',\
                              'artist_longitude').distinct()
    
    # write artists table to parquet files
    artists_table.write.mode('overwrite').parquet(path=output_data + 'artists')
    
    
    
    
def process_log_data(spark, input_data, output_data):
        
    """
    Reads the log_data JSON, and creates the songs and artists tables per specs. the tables are then
    written into the S3 bucket as parquet files. Finally, the fact table "songplays" is created by
    joining the songs table with the logs table.

    Arguments:
        spark:      The current spark session being used
        input_data: Directory path of where raw log_data exists. In this case it will be an S3 bucket
        input_data: Directory path of where processed data will be stored. In this case it will be an 
                    S3 bucket

    Returns:
        None
    """
        
    # get filepath to log data file
    log_data = os.path.join(input_data + 'log_data/*/*/*.json')
    
    
    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df.filter(df.page=='NextSong')
    
    
    # extract columns for users table    
    artists_table = df.select('userId', 'firstName', 'lastName', 'gender', 'level').distinct()
    
    # extract columns for users table    
    users_table = df.select(col("userId").alias("user_id"),\
                            col("firstName").alias("first_name"),\
                            col("lastName").alias("last_name"),\
                            "gender",\
                            "level").distinct()
    
    # write users table to parquet files
    users_table.write.mode('overwrite').parquet(path=output_data + 'users')
    
    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: x / 1000, TimestampType())
    df = df.withColumn("timestamp", get_timestamp(df.ts))
    
    # create datetime column from original timestamp column
    get_datetime = udf(lambda x: datetime.fromtimestamp(x), TimestampType())
    df = df.withColumn("start_time", get_datetime(df.timestamp))
    
    # extract columns to create time table
                             
    df=df.withColumn('start_time'(df['ts']/1000).cast('timestamp'))
    df = df.withColumn('year', year(df['start_time']))
    df = df.withColumn('month', month(df['start_time']))
    df = df.withColumn('week', weekofyear(df['start_time']))
    df = df.withColumn('weekday', date_format(df['start_time'], 'E'))
    df = df.withColumn('day', dayofmonth(df['start_time']))
    df = df.withColumn('hour', hour(df['start_time']))
    df.createOrReplaceTempView('log_view')
    time_table = df.select('start_time', 'hour', 'day','week',\
                           'weekday', 'year', 'month').distinct() 

    
    
    # write time table to parquet files partitioned by year and month
    time_table.write.mode('overwrite').partitionBy('year', 'month').parquet(path=output_data + 'time')
    
    
    # read in song data to use for songplays table
    song_df = spark.sql("SELECT * FROM songs_table_view") 
    
    # extract columns from joined song and log datasets to create songplays table 
    
    songplays_table = df.join(song_df, (df.song == song_df.title)\
                                       & (df.artist == song_df.artist_name)\
                                       & (df.length == song_df.duration), "inner")\
                        .distinct()\
                        .select('start_time', 'userId', 'level', 'song_id',\
                                'artist_id', 'sessionId','location','userAgent',\
                                df['year'].alias('year'), df['month'].alias('month'))\
                        .withColumn("songplay_id", monotonically_increasing_id())
    
    
    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.mode('overwrite').partitionBy('year','month').parquet(path=output_data + 'songplays')
    
    
    


def main():
    
    """
    A spark session is created, and used to read in the logs and songs data.
    the tables are then generated by spark and stored onto the S3 buckets.

    Arguments:
    None

    Returns:
    None
    """
    
    
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://aws-logs-datalake-project-2022-us-west-2/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)
    
if __name__ == "__main__":
    main()
