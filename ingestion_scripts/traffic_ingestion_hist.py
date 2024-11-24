import boto3
import urllib.request
import datetime

# Initialize S3 client
s3_client = boto3.client('s3')

def lambda_handler(event, context):
    # Define the base URL for historical data
    base_url = "https://data.stadt-zuerich.ch/dataset/sid_dav_verkehrszaehlung_miv_od2031/resource/"
    
    # Mapping of years to their corresponding resource IDs
    resource_ids = {
        "2012": "resource_id_2012.csv",
        "2013": "resource_id_2013.csv",
        "2014": "resource_id_2014.csv",
        "2015": "resource_id_2015.csv",
        "2016": "resource_id_2016.csv",
        "2017": "resource_id_2017.csv",
        "2018": "resource_id_2018.csv",
        "2019": "resource_id_2019.csv",
        "2020": "resource_id_2020.csv",
        "2021": "resource_id_2021.csv",
        "2022": "resource_id_2022.csv",
        "2023": "resource_id_2023.csv"
    }
    
    # S3 bucket name
    bucket_name = "traffic-data-lake"  # Replace with your S3 bucket name
    
    try:
        for year, file_name in resource_ids.items():
            # Construct the full URL
            full_url = f"{base_url}{file_name}"
            
            # Define the S3 file name
            s3_file_name = f"historical/{year}.csv"
            
            print(f"Fetching data for {year} from {full_url}")
            
            # Fetch the data using urllib
            with urllib.request.urlopen(full_url) as response:
                if response.status != 200:
                    print(f"Failed to fetch data for {year}. HTTP Status Code: {response.status}")
                    continue
                
                # Read the CSV data
                csv_data = response.read()
                
                # Save the data to S3
                s3_client.put_object(
                    Bucket=bucket_name,
                    Key=s3_file_name,
                    Body=csv_data,
                    ContentType='text/csv'
                )
                print(f"Data for {year} saved to S3 as {s3_file_name}")
        
        return {
            "statusCode": 200,
            "body": "Historical data fetched and saved to S3."
        }
    
    except Exception as e:
        print(f"Error: {str(e)}")
        return {
            "statusCode": 500,
            "body": str(e)
        }
