import json
import urllib.request
import boto3
from botocore.exceptions import ClientError
from datetime import datetime, timedelta

# Initialize S3 client
s3_client = boto3.client('s3')

def lambda_handler(event, context):
    bucket_name = "air-quality-data-lake"
    prefix = "all-data-aq/"

    try:
        # List all objects in the S3 bucket folder
        response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
        files = [obj['Key'] for obj in response.get('Contents', [])]

        for file_key in files:
            try:
                # Fetch the JSON file from S3
                data = s3_client.get_object(Bucket=bucket_name, Key=file_key)
                json_data = json.loads(data['Body'].read().decode('utf-8'))  # Decode JSON

                # Process the JSON data (modify this part as needed)
                print(f"Processing file: {file_key}")
                # For example, re-saving the JSON data to ensure it's properly formatted
                s3_client.put_object(
                    Bucket=bucket_name,
                    Key=file_key,
                    Body=json.dumps(json_data, indent=4),  # Re-save with pretty formatting
                    ContentType='application/json'
                )
            except json.JSONDecodeError:
                print(f"Failed to decode JSON for file: {file_key}")
            except ClientError as e:
                print(f"Error fetching from S3: {e}")

        return {"statusCode": 200, "body": "JSON files processed successfully."}

    except Exception as e:
        print(f"Unhandled error: {e}")
        return {"statusCode": 500, "body": str(e)}