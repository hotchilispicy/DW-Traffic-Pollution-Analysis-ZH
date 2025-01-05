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

    # Initial start date
    initial_start_date = datetime(2024, 12, 1)

    # Check current date and adjust start date accordingly
    today = datetime.now()
    if today < initial_start_date:
        start_date = initial_start_date
    else:
        start_date = today.replace(day=1, hour=0, minute=0, second=0, microsecond=0)

    # Calculate the end of the current month
    month_end_date = (start_date + timedelta(days=31)).replace(day=1) - timedelta(days=1)

    # Build the API URL
    url = f"{api_url}?latitude={latitude}&longitude={longitude}&hourly={hourly}&start_date={start_date.strftime('%Y-%m-%d')}&end_date={month_end_date.strftime('%Y-%m-%d')}"
    print("Requesting URL:", url)  # Log the URL for debugging

    # Fetch data from the API
    try:
        with urllib.request.urlopen(url) as response:
            if response.status != 200:
                print(f"Error fetching data: HTTP {response.status}")
                return {"statusCode": response.status, "body": "Failed to fetch data"}

            json_data = json.loads(response.read().decode('utf-8'))  

        # Save the JSON data to S3
        file_name = f"{folder}/{start_date.strftime('%Y-%m')}.json"  
        s3_client.put_object(
            Bucket=bucket_name,
            Key=file_name,
            Body=json.dumps(json_data),
            ContentType="application/json"
        )
        print(f"Saved data to {file_name}")
        return {"statusCode": 200, "body": "Monthly JSON fetched and saved to S3."}

    except Exception as e:
        print(f"Error handling API response: {e}")
        return {"statusCode": 500, "body": str(e)}