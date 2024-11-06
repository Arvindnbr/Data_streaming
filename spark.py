import logging
from datetime import datetime
from uuid import uuid4
from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import Cluster
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType


logging.basicConfig(level=logging.INFO)



def create_keyspace(session):
    session.execute("""
        CREATE KEYSPACE IF NOT EXISTS spark_stream
        WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}
    """)
    logging.info("Keyspace created successfully")

def create_table(session):
    session.execute("""
        CREATE TABLE IF NOT EXISTS spark_stream.users (
            id UUID PRIMARY KEY,
            first_name TEXT,
            last_name TEXT,
            gender TEXT,
            address TEXT,
            username TEXT,
            age INT,
            pincode TEXT,
            email TEXT,
            registered_date TEXT,
            phone_number TEXT,
            image TEXT
        ); 
    """)
    logging.info("Table created successfully")

def insert_data(session, **kwargs):
    logging.info("Inserting data...")

    uid = kwargs.get('id', str(uuid4()))  
    first_name = kwargs.get('first_name')
    last_name = kwargs.get('last_name')
    gender = kwargs.get('gender')
    address = kwargs.get('address')
    username = kwargs.get('username')
    age = kwargs.get('age')
    pincode = kwargs.get('pincode')
    email = kwargs.get('email')
    registered_date = kwargs.get('registered_date')
    phone = kwargs.get('phone_number')
    picture = kwargs.get('image')

    try:
        session.execute("""
            INSERT INTO spark_stream.users (
                id, first_name, last_name, gender, address, username, age, pincode, email, registered_date, phone, picture
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (uid, first_name, last_name, gender, address, username, age, pincode, email, registered_date, phone, picture))

        logging.info(f"Data inserted for {username} with id: {uid}")

    except Exception as e:
        logging.error(f"Could not insert data due to {e}")

def spark_connection():
    try:
        spark = SparkSession.builder \
            .appName('SparkDataStreaming') \
            .config('spark.jars.packages', 'com.datastax.spark:spark-cassandra-connector_2.12:3.5.1,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3') \
                            .getOrCreate()

            # .config('spark.cassandra.connection.host', 'localhost') \
            # .config("spark.cassandra.connection.port", "9042") \
        
        spark.sparkContext.setLogLevel("ERROR")
        logging.info("Spark connection established successfully")
        return spark

    except Exception as e:
        logging.error(f"Could not establish Spark session due to {e}")
        return None

def kafka_connection(spark):
    try:
        #set localhost:9092 if running locally
        spark_df = spark.readStream \
            .format('kafka') \
            .option('kafka.bootstrap.servers', 'localhost:9092') \
            .option('subscribe', 'users_created') \
            .option('startingOffset', 'earliest') \
            .option("failOnDataLoss", "false") \
            .load()
        
        logging.info("Kafka DataFrame created successfully")
        return spark_df

    except Exception as e:
        logging.warning(f"Kafka DataFrame could not be created due to {e}")
        return None

def selection_from_kafka(spark_df):
    schema = StructType([
        StructField("id", StringType(), False),
        StructField("first_name", StringType(), False),
        StructField("last_name", StringType(), False),
        StructField("gender", StringType(), False),
        StructField("address", StringType(), False),
        StructField("username", StringType(), False),
        StructField("age", IntegerType(), False),
        StructField("pincode", StringType(), False),
        StructField("email", StringType(), False),
        StructField("registered_date", StringType(), False),
        StructField("phone_number", StringType(), False),
        StructField("image", StringType(), False),
    ])

    selection = spark_df.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col('value'), schema).alias('data')) \
        .select("data.*")
    
    return selection

def cassandra_connection():
    try:
        cluster = Cluster(['localhost'], port = 9045, protocol_version=5, connect_timeout=10)
        #localhost 9045 if running locally
        session = cluster.connect()
        logging.info("Cassandra connection established successfully")
        return session

    except Exception as e:
        logging.error(f"Could not connect to Cassandra cluster due to {e}")
        return None

if __name__ == "__main__":
    spark_con = spark_connection()
    spark_con.conf.set("spark.cassandra.connection.host", "localhost")
    spark_con.conf.set("spark.cassandra.connection.port", "9045")
    #set the connection to localhost 9045 if running locally
    if spark_con:
        spark_df = kafka_connection(spark_con)
        if spark_df:
            selection_df = selection_from_kafka(spark_df)
            session = cassandra_connection()

            if session:
                create_keyspace(session)
                create_table(session)
                print(selection_df)
                # # Start streaming and write to Cassandra
                streaming_query = (selection_df.writeStream \
                                       .format("org.apache.spark.sql.cassandra") \
                                       .outputMode("append") \
                                       .option("checkpointLocation", "/tmp/checkpoint") \
                                        .option("keyspace", "spark_stream") \
                                        .option("table", "users") \
                                        .start())
                
                streaming_query.awaitTermination()
