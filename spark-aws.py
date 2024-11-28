
import os
from pyspark.sql import SparkSession

import snowflake.connector

# Set sensitive credentials using environment variables for security
AWS_KEY_ID = ""
AWS_SECRET_KEY = ""


# Set HADOOP_HOME for Windows
os.environ['HADOOP_HOME'] = 'C:\\hadoop'


# Initialize SparkSession
spark = SparkSession.builder \
    .appName("S3Integration") \
    .config("spark.hadoop.fs.s3a.access.key", AWS_KEY_ID) \
    .config("spark.hadoop.fs.s3a.secret.key", AWS_SECRET_KEY) \
    .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.1,com.amazonaws:aws-java-sdk-bundle:1.12.262") \
    .config("spark.local.dir", "C:/spark-temp") \
    .config("spark.files.overwrite", "true") \
    .config("spark.hadoop.fs.s3a.disk.checks", "false") \
    .config("spark.hadoop.fs.s3a.buffer.dir", "C:/spark-temp/s3a-buffer") \
    .config("spark.hadoop.fs.s3a.fast.upload", "true") \
    .config("spark.hadoop.fs.s3a.fast.upload", "true") \
    .config("spark.hadoop.fs.s3a.fast.upload.buffer", "array")\
    .config("spark.hadoop.fs.s3a.block.size", "32M") \
    .config("spark.hadoop.fs.s3a.multipart.size", "32M") \
    .config("spark.hadoop.fs.s3a.threads.max", "10") \
    .config("spark.hadoop.io.file.buffer.size", "131072") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "true") \
    .config("spark.hadoop.fs.s3a.logging.level", "DEBUG") \
    .getOrCreate()

# Read data from S3
s3_bucket = "s3a://cloud-assignment-001/data.csv"
df = spark.read.csv(s3_bucket, header=True, inferSchema=True)

# Inspect schema
df.printSchema()

print("++++++++++++++++++++ Schema printed ++++++++++++++++++++++")

# Perform transformations
transformed_df = df.filter(df['Price'] > 5)  # Replace 'Price' with the actual column name if needed

# Save transformed data back to S3
output_path = "s3a://cloud-assignment-001/transformed-data/"
# transformed_df.write.mode("overwrite").parquet(output_path)
transformed_df.write \
    .mode("overwrite") \
    .option("header", "true") \
    .csv(output_path)

# Show transformed data
transformed_df.show()

print("+++++++++++++++ Done Transforming and saving Data back to s3 ++++++++++++++++++++++++++++")


# Snowflake connection
conn = snowflake.connector.connect(
    user='',
    password='',
    account='',
    warehouse='',
    database='',
    schema=''
)


# Create a cursor object
cursor = conn.cursor()

# Load data from S3 to Snowflake
cursor.execute(f"""
    COPY INTO DATA_01
    FROM 's3://cloud-assignment-001/transformed-data/'
    CREDENTIALS=(AWS_KEY_ID='{AWS_KEY_ID}' AWS_SECRET_KEY='{AWS_SECRET_KEY}')
    FILE_FORMAT = (TYPE = CSV, FIELD_OPTIONALLY_ENCLOSED_BY = '"', SKIP_HEADER = 1);
""")

# Closing the cursor and connection
cursor.close()
conn.close()

print("++++++++++++++  DAta Pushed to Warehouse ++++++++++++++++++") 



