import urllib.request
import boto3
import logging

# Set up logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Initialize S3 client
s3_client = boto3.client('s3')


def lambda_handler4(event, context):
    # Base URL for CSV files
    base_url = "https://data.stadt-zuerich.ch/dataset/ugz_verkehrsdaten_stundenwerte_stampfenbachstrasse/download/ugz_ogd_traffic_h1_"

    # The years for the dataset (2007 to 2023)
    years = list(range(2007, 2024))

    # S3 parameters
    bucket_name = "datazurich"  # Replace with your bucket name
    s3_prefix = "stampfenbach_street/"  # Optional folder structure in S3

    try:
        # Process each year by constructing the URL for each year
        for year in years:
            csv_url = f"{base_url}{year}.csv"
            file_name = f"{s3_prefix}ugz_ogd_traffic_h1_{year}.csv"

            # Fetch data from the CSV URL
            logger.info(f"Fetching data from {csv_url}.")
            with urllib.request.urlopen(csv_url) as response:
                if response.status != 200:
                    logger.error(f"Error fetching data from {csv_url}: HTTP {response.status}")
                    continue

                # Stream the data directly to S3
                s3_client.put_object(
                    Bucket=bucket_name,
                    Key=file_name,
                    Body=response.read(),
                    ContentType='text/csv'
                )
                logger.info(f"File {file_name} successfully uploaded to S3.")

        return {
            "statusCode": 200,
            "body": "All CSV files successfully fetched and uploaded to S3."
        }

    except Exception as e:
        logger.error(f"An error occurred: {e}")
        return {
            "statusCode": 500,
            "body": f"An error occurred: {e}"
        }
