import boto3
from datetime import datetime

def lambda_handler(event, context):
    # Define the end date (5th December 2024)
    end_date = datetime(2024, 12, 5)

    # Get the current date in UTC
    current_date = datetime.utcnow()

    # Compare the dates
    if current_date > end_date:
        client = boto3.client('events')
        
        # Replace 'your_rule_name' with the name of the rule to disable
        rule_name = 'your_rule_name'
        
        try:
            client.disable_rule(Name=rule_name)
            return {"status": "Rule disabled successfully"}
        except Exception as e:
            return {"status": "Failed to disable rule", "error": str(e)}
    else:
        return {"status": "Rule is still active, no action taken"}
