import boto3
import json
import time 
aws_access_key_id = 'AKIARN4SUSM6BSKI7ZJP'
aws_secret_access_key = '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'
aws_region = 'us-east-1'
import os

# Get the path to the Downloads folder
downloads_folder_path = os.path.expanduser("C:\\Users\\rajya\\Desktop\\Downloads")

# Locate the JSON file
json_file_path = os.path.join(downloads_folder_path, "ecommerce_data.json")

# Check if the JSON file exists
if os.path.exists(json_file_path):
    # Open the JSON file
    with open(json_file_path, "r") as json_file:
        product_data = json.load(json_file)
kinesis_client = boto3.client('kinesis', region_name='us-east-1', aws_access_key_id='AKIARN4SUSM6BSKI7ZJP', aws_secret_access_key='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')
stream_name = 'data-streamer'
shard_count = 1  # You can adjust the number of shards as needed

try:
    kinesis_client.create_stream(
        StreamName=stream_name,
        ShardCount=shard_count
    )
    print(f"Stream '{stream_name}' created successfully with {shard_count} shard(s).")
except kinesis_client.exceptions.ResourceInUseException:
    print(f"Stream '{stream_name}' already exists.")
except Exception as e:
    print(f"Error creating stream: {str(e)}")
for product in product_data:
    # Convert the product record to JSON
    product_json = json.dumps(product)

    # Send the JSON record to the stream
    response = kinesis_client.put_record(
        StreamName=stream_name,
        Data=product_json,
        PartitionKey=product['date']  # Use a unique identifier as the partition key
    )

    print(f"Record sent to Kinesis: {response['SequenceNumber']},{response['Sha    



