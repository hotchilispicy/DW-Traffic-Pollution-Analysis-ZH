import urllib.request
import boto3
from datetime import datetime, timedelta

# initialize S3 client
s3_client = boto3.client('s3')

def lambda_handler(event, context):
    # API URL and parameters
    api_url = "https://air-quality-api.open-meteo.com/v1/air-quality"
    latitude = 47.3769  # Zurich latitude
    longitude = 8.5417  # Zurich longitude
    hourly = "pm10,pm2_5"
    bucket_name = "air-quality-data-lake" 
    folder = "historical"  # Folder in the S3 bucket

    # data range
    start_date = datetime(2020, 1, 1)
    end_date = datetime(2023, 10, 31)

    try:
        current_date = start_date
        while current_date <= end_date:
            # calculate the end of the current month
            month_end_date = datetime(current_date.year, current_date.month, 1) + timedelta(days=31)
            month_end_date = month_end_date.replace(day=1) - timedelta(days=1)

            # Adjust if the current month exceeds the overall end_date
            month_end_date = min(month_end_date, end_date)

            # Build the API URL
            params = (
                f"?latitude={latitude}&longitude={longitude}"
                f"&hourly={hourly}"
                f"&start_date={current_date.strftime('%Y-%m-%d')}"
                f"&end_date={month_end_date.strftime('%Y-%m-%d')}"
            )
            url = api_url + params

            # fetch data from the API
            with urllib.request.urlopen(url) as response:
                if response.status != 200:
                    print(f"Error fetching data: HTTP {response.status}")
                    return {"statusCode": response.status, "body": "Failed to fetch data"}

                csv_data = response.read()

            # save the CSV data to S3
            file_name = f"{folder}/{current_date.strftime('%Y-%m')}.csv"  # Filename by month
            s3_client.put_object(
                Bucket=bucket_name,
                Key=file_name,
                Body=csv_data,
                ContentType="text/csv"
            )
            print(f"Saved data to {file_name}")

            # move to the next month
            current_date = datetime(current_date.year, current_date.month, 1) + timedelta(days=31)
            current_date = current_date.replace(day=1)

        return {"statusCode": 200, "body": "Monthly CSVs fetched and saved to S3."}

    except Exception as e:
        print(f"Error: {e}")
        return {"statusCode": 500, "body": str(e)}
