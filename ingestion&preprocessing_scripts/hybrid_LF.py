import urllib.request
import boto3
import pandas as pd
import io
import logging

# Set up logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Initialize S3 client
s3_client = boto3.client('s3')


def lambda_handler8(event, context):
    # Direct download URL for Zurich dataset CSV
    api_url = "https://www.web.statistik.zh.ch/ogd/data/KANTON_ZUERICH_606.csv"

    # S3 parameters
    bucket_name = "datazurich"  # Replace with your bucket name
    file_name = "zurich_fleet/hybrid.csv"

    try:
        # Fetch data from Zurich dataset
        logger.info("Fetching data from Zurich dataset.")
        with urllib.request.urlopen(api_url) as response:
            if response.status != 200:
                logger.error(f"Error fetching data: HTTP {response.status}")
                return {"statusCode": response.status, "body": "Failed to fetch data"}

            csv_data = response.read().decode('utf-8')
            logger.info("Data successfully fetched from Zurich dataset.")

        # Load the CSV data into a DataFrame
        df_new = pd.read_csv(io.StringIO(csv_data))
        logger.info(f"Fetched {len(df_new)} rows of new data.")

        # Save the data directly to S3
        csv_buffer = io.StringIO()
        df_new.to_csv(csv_buffer, index=False)
        s3_client.put_object(
            Bucket=bucket_name,
            Key=file_name,
            Body=csv_buffer.getvalue(),
            ContentType="text/csv"
        )
        logger.info(f"File saved to {file_name} in S3.")

        return {"statusCode": 200, "body": f"Data saved in {file_name}"}

    except Exception as e:
        logger.error(f"Error occurred: {e}")
        return {"statusCode": 500, "body": str(e)}
