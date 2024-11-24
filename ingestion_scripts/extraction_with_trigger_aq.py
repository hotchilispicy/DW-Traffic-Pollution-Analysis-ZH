import urllib.request
import boto3
import pandas as pd
from datetime import datetime, timedelta
import io
import logging

# Set up logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# initialize S3 client
s3_client = boto3.client('s3')

def lambda_handler(event, context):
    # API parameters
    api_url = "https://air-quality-api.open-meteo.com/v1/air-quality"
    latitude = 47.3769  # Zurich latitude
    longitude = 8.5417  # Zurich longitude
    hourly = "pm10,pm2_5"
    bucket_name = "air-quality-data-lake"
    file_name = "historical/2024-11.csv"  
    
    # data range
    today = datetime.utcnow()
    start_date = datetime(2024, 11, 1)
    end_of_month = datetime(2024, 11, 30)

    try:
        # Check if the file already exists in S3
        try:
            s3_object = s3_client.get_object(Bucket=bucket_name, Key=file_name)
            existing_csv = s3_object['Body'].read().decode('utf-8')
            df_existing = pd.read_csv(io.StringIO(existing_csv))
            logger.info("Existing data loaded successfully from S3.")
        except s3_client.exceptions.NoSuchKey:
            # If the file does not exist, create an empty DataFrame
            logger.warning(f"No existing file found in S3 for {file_name}. Creating a new file.")
            df_existing = pd.DataFrame(columns=['time', 'pm10', 'pm2_5']) 
            
        # Determine the date range to fetch
        if today > end_of_month:
            logger.info("November 2024 has ended. No further updates required.")
            return {"statusCode": 200, "body": "No updates needed. End of November 2024 reached."}

        fetch_start_date = (
            max(pd.to_datetime(df_existing['time']).max(), today).strftime('%Y-%m-%d')
            if not df_existing.empty else start_date.strftime('%Y-%m-%d')
        )
        fetch_end_date = today.strftime('%Y-%m-%d')

        logger.info(f"Fetching data from {fetch_start_date} to {fetch_end_date}.")

        params = (
            f"?latitude={latitude}&longitude={longitude}"
            f"&hourly={hourly}"
            f"&start_date={fetch_start_date}"
            f"&end_date={fetch_end_date}"
        )
        url = api_url + params

        # fetch data from the API
        with urllib.request.urlopen(url) as response:
            if response.status != 200:
                logger.error(f"Error fetching data: HTTP {response.status}")
                return {"statusCode": response.status, "body": "Failed to fetch data"}
            
            csv_data = response.read().decode('utf-8')
            logger.info("Data successfully fetched from API.")

        # add ad process new data from the trigger
        new_data = pd.read_csv(io.StringIO(csv_data))
        logger.info(f"Fetched {len(new_data)} rows of new data.")

        # append new data to the existing DataFrame
        df_combined = pd.concat([df_existing, new_data]).drop_duplicates(subset=['time'])
        logger.info("New data appended to the existing dataset.")

        # save the updated CSV back to S3
        csv_buffer = io.StringIO()
        df_combined.to_csv(csv_buffer, index=False)
        s3_client.put_object(
            Bucket=bucket_name,
            Key=file_name,
            Body=csv_buffer.getvalue(),
            ContentType="text/csv"
        )
        logger.info(f"Updated file saved to {file_name} in S3.")

        return {"statusCode": 200, "body": f"Data fetched and updated in {file_name}"}

    except Exception as e:
        logger.error(f"Error occurred: {e}")
        return {"statusCode": 500, "body": str(e)}