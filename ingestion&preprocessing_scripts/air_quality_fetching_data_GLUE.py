import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import boto3

# Parse arguments
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# S3 Paths
input_folder = "s3://air-quality-data-lake/processed/"  # Input folder containing CSVs
output_folder = "s3://air-quality-data-lake/merged_csv/"    # Output folder for merged CSVs

# Initialize S3 client
s3 = boto3.client('s3')
bucket_name = "air-quality-data-lake"
prefix = "processed/"

# List all CSV files in the folder
response = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
files = [obj['Key'] for obj in response.get('Contents', []) if obj['Key'].endswith('.csv')]

# Debug: Print all files found
print(f"All CSV files found in {prefix}: {files}")

# Process files for each year
years = ["2020", "2021", "2022", "2023", "2024"]
for year in years:
    # Filter files for the current year
    year_files = [file_key for file_key in files if file_key.split('/')[-1].startswith(year)]

    # Debug: Print files being processed for the year
    print(f"Processing files for year {year}: {year_files}")

    # Read and merge files for the year
    merged_df = None
    for file_key in year_files:
        input_path = f"s3://{bucket_name}/{file_key}"

        # Load CSV as DataFrame
        df = spark.read.option("header", "true").csv(input_path)

        # Merge DataFrames
        if merged_df is None:
            merged_df = df
        else:
            merged_df = merged_df.union(df)

    # Write merged DataFrame to a single CSV for the year
    if merged_df is not None:
        output_path = f"{output_folder}{year}.csv"
        merged_df.coalesce(1).write.mode("overwrite").option("header", "true").csv(output_path)
        print(f"Merged CSV saved for year {year} at {output_path}")
    else:
        print(f"No files found for year {year}")

# Commit the Glue job
job.commit()
