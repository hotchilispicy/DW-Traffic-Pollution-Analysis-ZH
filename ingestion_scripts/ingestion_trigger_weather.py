import urllib.request
import json
import boto3

# S3 Configuration
S3_BUCKET_NAME = "weather-data-lake"
S3_FOLDER = "historical"

# Base URL for CKAN
CKAN_BASE_URL = "https://data.stadt-zuerich.ch/api/3/action"

def fetch_dataset_metadata(dataset_id):
    """
    Fetch metadata for the given dataset.
    """
    url = f"{CKAN_BASE_URL}/package_show?id={dataset_id}"
    try:
        with urllib.request.urlopen(url) as response:
            if response.status == 200:
                data = response.read().decode("utf-8")
                return json.loads(data)
    except urllib.error.HTTPError as e:
        print(f"HTTP Error: {e.code} - {e.reason}")
        return None
    except urllib.error.URLError as e:
        print(f"URL Error: {e.reason}")
        return None

def download_and_upload_file(resource_url, file_name):
    """
    Download a file from a URL and upload it to an S3 bucket.
    """
    try:
        with urllib.request.urlopen(resource_url) as response:
            file_data = response.read()

            # Upload to S3
            s3 = boto3.client("s3")
            s3.put_object(
                Bucket=S3_BUCKET_NAME,
                Key=f"{S3_FOLDER}/{file_name}",
                Body=file_data
            )
            print(f"Uploaded {file_name} to S3 bucket {S3_BUCKET_NAME}/{S3_FOLDER}")
    except Exception as e:
        print(f"Error downloading or uploading file {file_name}: {str(e)}")

def lambda_handler(event, context):
    """
    Main Lambda function to fetch and upload dataset files to S3.
    """
    dataset_id = "ugz_meteodaten_stundenmittelwerte"  # The dataset ID from the URL
    metadata = fetch_dataset_metadata(dataset_id)

    if not metadata or not metadata.get("result"):
        return {"statusCode": 500, "body": "Failed to fetch dataset metadata"}

    resources = metadata["result"]["resources"]
    for resource in resources:
        resource_url = resource.get("url")
        file_name = resource.get("name")
        if resource_url and file_name:
            print(f"Processing file: {file_name}")
            download_and_upload_file(resource_url, file_name)

    return {"statusCode": 200, "body": "Files successfully processed and uploaded to S3"}
