import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.functions import col, when

# Parse arguments
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Input S3 file path
input_folder = "s3://weather-data-lake/processed/ugz_ogd_meteo_h1_2020.csv"

# MySQL Database Details
jdbc_url = "jdbc:mysql://air-quality-db.c6jid8c0mhxf.us-east-1.rds.amazonaws.com:3306/weather_data"
username = "admin"
password = "aqdata.09"

# Load CSV file into DynamicFrame
datasource = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    connection_options={"paths": [input_folder]},
    format="csv",
    format_options={"withHeader": True}
)

# Convert to DataFrame for additional transformations
df = datasource.toDF()

# Ensure valid data in the 'Wert' column
# Replace invalid or excessively large values with NULL, but keep valid 0.0 values
cleaned_df = df.withColumn(
    "Wert",
    when(
        col("Wert").cast("float").isNull() | (col("Wert") > 1e6) | (col("Wert") < -1e6),  # Example range check
        None
    ).otherwise(col("Wert").cast("float"))
)

# Log the schema of the transformed DataFrame
cleaned_df.printSchema()
cleaned_df.show(10)

# Convert back to DynamicFrame
cleaned_dynamic_frame = DynamicFrame.fromDF(cleaned_df, glueContext, "cleaned_df")

# Write to MySQL
glueContext.write_dynamic_frame.from_options(
    frame=cleaned_dynamic_frame,
    connection_type="jdbc",
    connection_options={
        "url": jdbc_url,
        "user": username,
        "password": password,
        "dbtable": "weather_2020",
    },
    transformation_ctx="write_to_mysql"
)

print("Data successfully loaded into MySQL database.")
job.commit()
