import json
import urllib.request
import boto3
from datetime import datetime, timedelta

# Initialize S3 client
s3_client = boto3.client('s3')

def lambda_handler(event, context):
    # API URL and parameters
    api_url = "https://air-quality-api.open-meteo.com/v1/air-quality"
    latitude = 47.3769  # Zurich latitude
    longitude = 8.5417  # Zurich longitude
    hourly = "pm10,pm2_5,carbon_monoxide,carbon_dioxide,nitrogen_dioxide,sulphur_dioxide,ozone,aerosol_optical_depth,dust,uv_index,uv_index_clear_sky,ammonia,methane"
    bucket_name = "air-quality-data-lake"
    folder = "all-data-aq"  # Folder in the S3 bucket

    # Data range
    start_date = datetime(2020, 1, 1)
    end_date = datetime(2024, 10, 31)

    try:
        current_date = start_date
        while current_date <= end_date:
            # Calculate the end of the current month
            month_end_date = datetime(current_date.year, current_date.month, 1) + timedelta(days=31)
            month_end_date = month_end_date.replace(day=1) - timedelta(days=1)

            # Adjust if the current month exceeds the overall end_date
            month_end_date = min(month_end_date, end_date)

            # Build the API URL
            url = f"{api_url}?latitude={latitude}&longitude={longitude}&hourly={hourly}&start_date={current_date.strftime('%Y-%m-%d')}&end_date={month_end_date.strftime('%Y-%m-%d')}"
            print("Requesting URL:", url)  # Log the URL for debugging

            # Fetch data from the API
            with urllib.request.urlopen(url) as response:
                if response.status != 200:
                    print(f"Error fetching data: HTTP {response.status}")
                    return {"statusCode": response.status, "body": "Failed to fetch data"}

                json_data = json.loads(response.read().decode('utf-8'))  # Decode and convert to JSON

            # Save the JSON data to S3
            file_name = f"{folder}/{current_date.strftime('%Y-%m')}.json"  # Filename by month, changed to .json
            s3_client.put_object(
                Bucket=bucket_name,
                Key=file_name,
                Body=json.dumps(json_data),  # Convert dict to JSON formatted string
                ContentType="application/json"
            )
            print(f"Saved data to {file_name}")

            # Move to the next month
            current_date = datetime(current_date.year, current_date.month, 1) + timedelta(days=31)
            current_date = current_date.replace(day=1)

        return {"statusCode": 200, "body": "Monthly JSONs fetched and saved to S3."}

    except Exception as e:
        print(f"Error: {e}")
        return {"statusCode": 500, "body": str(e)}
