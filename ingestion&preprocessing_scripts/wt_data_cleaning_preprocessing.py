import boto3
import pandas as pd
import io
import re

# Initialize S3 client
s3_client = boto3.client('s3')

def lambda_handler(event, context):
    # Input and output bucket names
    input_bucket = 'weather-data-lake'
    output_bucket = 'weather-data-lake'
    input_prefix = 'all-data-wt/'  # Folder where the files are located
    output_prefix = 'processed/'  # Folder where cleaned files will be saved

    try:
        # List all files in the input folder
        response = s3_client.list_objects_v2(Bucket=input_bucket, Prefix=input_prefix)
        if 'Contents' not in response:
            return {'statusCode': 404, 'body': 'No files found in the specified folder.'}

        # Iterate over all CSV files in the folder
        for obj in response['Contents']:
            file_key = obj['Key']
            if not file_key.endswith('.csv'):
                continue  # Skip non-CSV files

            # Extract year from filename (assuming year is in the format '_YYYY.csv')
            match = re.search(r'_([0-9]{4})\.csv$', file_key)
            if not match:
                print(f"Skipping file (no year found): {file_key}")
                continue

            year = int(match.group(1))
            if year <= 2019:
                print(f"Skipping file (year <= 2019): {file_key}")
                continue

            print(f"Processing file (year > 2019): {file_key}")

            # Download the file from S3
            file_obj = s3_client.get_object(Bucket=input_bucket, Key=file_key)
            csv_content = file_obj['Body'].read().decode('utf-8')

            # Load CSV into Pandas DataFrame
            df = pd.read_csv(io.StringIO(csv_content), quotechar='"')

            # Remove unnecessary quotes from column names
            df.columns = [col.strip('"') for col in df.columns]

            # Clean each cell value by stripping extra quotes
            df = df.applymap(lambda x: x.strip('"') if isinstance(x, str) else x)

            # Convert 'Datum' to SQL DATETIME format (remove timezone)
            if 'Datum' in df.columns:
                df['Datum'] = pd.to_datetime(df['Datum'].str.split('+').str[0]).dt.strftime('%Y-%m-%d %H:%M:%S')

            # Remove special characters from 'Einheit' column
            if 'Einheit' in df.columns:
                df['Einheit'] = df['Einheit'].apply(
                    lambda x: re.sub(r'[^\w\s]', '', x) if isinstance(x, str) else x
                )

            # Save the cleaned DataFrame to a new CSV
            output_csv_buffer = io.StringIO()
            df.to_csv(output_csv_buffer, index=False)

            # Upload the cleaned CSV to the specified output folder
            output_key = f"{output_prefix}{file_key.split('/')[-1]}"
            s3_client.put_object(
                Bucket=output_bucket,
                Key=output_key,
                Body=output_csv_buffer.getvalue()
            )

            print(f"File successfully cleaned and saved to {output_bucket}/{output_key}")

        return {'statusCode': 200, 'body': f"All files processed and saved to {output_prefix}"}

    except Exception as e:
        return {'statusCode': 500, 'body': f"An error occurred: {str(e)}"}
