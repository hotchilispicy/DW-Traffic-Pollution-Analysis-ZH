import boto3
import pandas as pd
import json
import io

# Initialize S3 client
s3 = boto3.client('s3')

# Define the bucket and folders
input_bucket = "air-quality-data-lake"
input_folder = "all-data-aq/"
output_folder = "processed/"

def lambda_handler(event, context):
    # List all JSON files in the input folder
    response = s3.list_objects_v2(Bucket=input_bucket, Prefix=input_folder)
    files = [obj['Key'] for obj in response.get('Contents', []) if obj['Key'].endswith('.json')]

    # Debug: Log the files found
    print(f"Files found: {files}")

    for file_key in files:
        try:
            # Read the JSON file directly from S3
            obj = s3.get_object(Bucket=input_bucket, Key=file_key)
            json_data = json.loads(obj['Body'].read().decode('utf-8'))

            # Process the 'hourly' data into a Pandas DataFrame
            hourly_data = json_data['hourly']
            df = pd.DataFrame({
                'time': hourly_data['time'],
                'pm10': hourly_data['pm10'],
                'pm2_5': hourly_data['pm2_5'],
                'carbon_monoxide': hourly_data['carbon_monoxide'],
                'carbon_dioxide': hourly_data['carbon_dioxide'],
                'nitrogen_dioxide': hourly_data['nitrogen_dioxide'],
                'sulphur_dioxide': hourly_data['sulphur_dioxide'],
                'ozone': hourly_data['ozone'],
                'aerosol_optical_depth': hourly_data['aerosol_optical_depth'],
                'dust': hourly_data['dust'],
                'uv_index': hourly_data['uv_index'],
                'uv_index_clear_sky': hourly_data['uv_index_clear_sky'],
                'ammonia': hourly_data['ammonia'],
                'methane': hourly_data['methane'],
            })

            # Add metadata columns
            df['latitude'] = json_data['latitude']
            df['longitude'] = json_data['longitude']
            df['generationtime_ms'] = json_data['generationtime_ms']
            df['utc_offset_seconds'] = json_data['utc_offset_seconds']
            df['timezone'] = json_data['timezone']
            df['timezone_abbreviation'] = json_data['timezone_abbreviation']
            df['elevation'] = json_data['elevation']

            # Convert DataFrame to CSV in memory
            csv_buffer = io.StringIO()
            df.to_csv(csv_buffer, index=False)

            # Upload the CSV to the S3 output folder
            output_key = f"{output_folder}{file_key.split('/')[-1].replace('.json', '.csv')}"
            s3.put_object(Bucket=input_bucket, Key=output_key, Body=csv_buffer.getvalue())

            print(f"Processed and uploaded: {output_key}")
        
        except Exception as e:
            print(f"Failed to process file {file_key}: {e}")

    return {
        'statusCode': 200,
        'body': f"Processed {len(files)} files successfully."
    }
