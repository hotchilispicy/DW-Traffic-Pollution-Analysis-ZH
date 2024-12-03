import urllib.request
import json
import boto3
from datetime import datetime, timedelta

# CKAN API Base URL
CKAN_BASE_URL = "https://data.stadt-zuerich.ch/api/3/action"
RESOURCE_ID = "1c2c29d2-f37a-4c31-95df-321286541360"  # Replace with your resource ID

# S3 Configuration
S3_BUCKET_NAME = "weather-data-lake"
S3_FOLDER = "historical"

# Fetch metadata for the resource
def fetch_resource_metadata(resource_id):
    url = f"{CKAN_BASE_URL}/package_show?id=ugz_meteodaten_stundenmittelwerte"
    try:
        with urllib.request.urlopen(url) as response:
            if response.status == 200:
                data = response.read().decode("utf-8")
                metadata = json.loads(data)
                for resource in metadata["result"]["resources"]:
                    if resource["id"] == resource_id:
                        return resource
    except Exception as e:
        print(f"Error fetching resource metadata: {e}")
        return None

# Fetch data from the resource
def fetch_data(resource_id, start_date, end_date):
    url = f"{CKAN_BASE_URL}/datastore_search?resource_id={resource_id}&filters={{\"Datum\": {{\"$gte\": \"{start_date}\", \"$lt\": \"{end_date}\"}}}}"
    try:
        with urllib.request.urlopen(url) as response:
            if response.status == 200:
                data = response.read().decode("utf-8")
                return json.loads(data)["result"]["records"]
    except Exception as e:
        print(f"Error fetching data: {e}")
        return []

# Upload to S3
def upload_to_s3(file_name, file_data):
    s3 = boto3.client("s3")
    s3.put_object(
        Bucket=S3_BUCKET_NAME,
        Key=f"{S3_FOLDER}/{file_name}",
        Body=file_data
    )
    print(f"Uploaded {file_name} to S3 bucket {S3_BUCKET_NAME}/{S3_FOLDER}")

# Lambda handler function
def lambda_handler(event, context):
    # Step 1: Fetch metadata
    resource_metadata = fetch_resource_metadata(RESOURCE_ID)
    if not resource_metadata:
        return {"statusCode": 500, "body": "Failed to fetch resource metadata"}
    
    # Step 2: Compare last modified date
    last_modified = resource_metadata.get("last_modified", "")
    if not last_modified:
        return {"statusCode": 500, "body": "No last_modified field in metadata"}
    
    last_modified_date = datetime.fromisoformat(last_modified.replace("Z", "+00:00"))
    current_time = datetime.now()

    # Check if the last modified date is in the current month
    if last_modified_date.year != current_time.year or last_modified_date.month != current_time.month:
        return {"statusCode": 200, "body": "No updates for the current month"}

    # Step 3: Fetch only data for the current month
    start_date = current_time.replace(day=1).strftime("%Y-%m-%d")
    end_date = (current_time + timedelta(days=31)).replace(day=1).strftime("%Y-%m-%d")
    
    updated_data = fetch_data(RESOURCE_ID, start_date, end_date)
    if not updated_data:
        return {"statusCode": 200, "body": "No new data to update"}
    
    # Step 4: Prepare data for S3
    file_name = f"updated_data_{start_date}_to_{end_date}.json"
    file_data = json.dumps(updated_data)

    # Step 5: Upload updated data to S3
    upload_to_s3(file_name, file_data)

    return {"statusCode": 200, "body": "Updates successfully processed and uploaded"}
