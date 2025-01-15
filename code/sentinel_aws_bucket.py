import boto3
import os
import psycopg2

# S3 Configuration
BUCKET_NAME = "sentinel-s2-l2a"
LOCAL_DIRECTORY = r"C:\Users\Javier\Desktop\CROP\Sentinel2_trial"

# Create the directory if it doesn't exist
os.makedirs(LOCAL_DIRECTORY, exist_ok=True)

# PostgreSQL Connection
conn = psycopg2.connect(
    dbname='crop-health-db',
    user='master',
    password='JFKae$2341',
    host='crop-health-db.cv0iskeoocuw.eu-north-1.rds.amazonaws.com',
    port='5432'
)
cursor = conn.cursor()

# Step 1: Retrieve the `sentinel2_query` value from the first row of the `fields` table
cursor.execute("SELECT sentinel2_query FROM fields LIMIT 1;")
result = cursor.fetchone()

if result:
    sentinel2_query = result[0]
    print(f"Retrieved Sentinel-2 query: {sentinel2_query}")

    # Step 2: Construct file paths
    FILES_TO_DOWNLOAD = [
        f"{sentinel2_query}2023/12/4/0/qi/CLD_20m.jp2",
        f"{sentinel2_query}2023/12/4/0/R10m/B08.jp2",
        f"{sentinel2_query}2023/12/4/0/R10m/TCI.jp2"
    ]

    # Step 3: Initialize S3 client and download files
    s3_client = boto3.client("s3")

    for file_key in FILES_TO_DOWNLOAD:
        local_file_path = os.path.join(LOCAL_DIRECTORY, os.path.basename(file_key))
        try:
            print(f"Downloading {file_key} to {local_file_path}...")
            s3_client.download_file(BUCKET_NAME, file_key, local_file_path)
            print(f"Downloaded {file_key} successfully.")
        except Exception as e:
            print(f"Error downloading {file_key}: {e}")
else:
    print("No rows found in the fields table.")

# Close the database connection
cursor.close()
conn.close()
