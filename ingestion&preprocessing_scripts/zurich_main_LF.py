import urllib.request
import boto3
import logging

# Set up logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Initialize S3 client
s3_client = boto3.client('s3')

def lambda_handler5(event, context):
    # Base URL for CSV files with varying years
    base_url = "https://data.stadt-zuerich.ch/dataset/sid_dav_verkehrszaehlung_miv_od2031/download/sid_dav_verkehrszaehlung_miv_OD2031_"

    # The years for the dataset (2012 to 2024)
    years = list(range(2012, 2025))

    # S3 parameters
    bucket_name = "datazurich"  # Replace with your bucket name
    s3_prefix = "traffic_data/"  # Optional folder structure in S3

    try:
        for year in years:
            csv_url = f"{base_url}{year}.csv"
            file_name = f"{s3_prefix}sid_dav_verkehrszaehlung_miv_OD2031_{year}.csv"

            # Open the URL as a stream
            logger.info(f"Fetching data from {csv_url}.")
            with urllib.request.urlopen(csv_url) as response:
                if response.status != 200:
                    logger.error(f"Failed to fetch data from {csv_url}: HTTP {response.status}")
                    continue

                # Stream the data directly to S3
                s3_client.put_object(
                    Bucket=bucket_name,
                    Key=file_name,
                    Body=response.read(),
                    ContentType='text/csv'
                )
                logger.info(f"File saved to {file_name} in S3.")

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
