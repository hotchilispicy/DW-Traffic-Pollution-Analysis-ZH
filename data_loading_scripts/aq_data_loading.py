import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.functions import lit, when, col

# Parse arguments
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Input S3 file path for 2020 data
input_folder = "s3://air-quality-data-lake/merged_csv/2024.csv/"

# MySQL Database Details
jdbc_url = "jdbc:mysql://air-quality-db.c6jid8c0mhxf.us-east-1.rds.amazonaws.com:3306/air_quality"
username = "admin"  # MySQL username
password = "aqdata.09"  # MySQL password

# Step 1: Load CSV file from S3 into a DynamicFrame
datasource = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    connection_options={"paths": [input_folder]},
    format="csv",
    format_options={"withHeader": True}
)

# Step 2: Convert to DataFrame for cleaning
df = datasource.toDF()

# Step 3: Handle empty `carbon_dioxide` and `aerosol_optical_depth` columns
# Replace empty or null values with `NULL` to match MySQL expectations
cleaned_df = df \
    .withColumn(
        "carbon_dioxide",
        when(col("carbon_dioxide").isNull() | (col("carbon_dioxide") == ""), None).otherwise(col("carbon_dioxide"))
    ) \
    .withColumn(
        "aerosol_optical_depth",
        when(col("aerosol_optical_depth").isNull() | (col("aerosol_optical_depth") == ""), None).otherwise(col("aerosol_optical_depth"))
    ) \
    .withColumn(
        "uv_index",
        when(col("uv_index").isNull() | (col("uv_index") == ""), None).otherwise(col("uv_index"))
    ) \
    .withColumn(
        "uv_index_clear_sky",
        when(col("uv_index_clear_sky").isNull() | (col("uv_index_clear_sky") == ""), None).otherwise(col("uv_index_clear_sky"))
    ) \
    .withColumn(
        "methane",
        when(col("methane").isNull() | (col("methane") == ""), None).otherwise(col("methane"))    
    )

# Step 4: Convert back to DynamicFrame
cleaned_dynamic_frame = DynamicFrame.fromDF(cleaned_df, glueContext, "cleaned_df")

# Step 5: Write the data to MySQL
glueContext.write_dynamic_frame.from_options(
    frame=cleaned_dynamic_frame,
    connection_type="jdbc",
    connection_options={
        "url": jdbc_url,
        "user": username,
        "password": password,
        "dbtable": "air_quality_2024",
    },
    transformation_ctx="write_to_mysql"
)

print("Data successfully written to MySQL database.")

# Commit the job
job.commit()
