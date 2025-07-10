import pandas as pd
from faker import Faker
from google.cloud import storage
import os
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "C:\Program Files\Python313\coral-loop-463008-c9-dde51b4880ee.json"

import os

def generate_dummy_data(num_records=10):
    """
    Generates a specified number of dummy data records including PII fields.

    Args:
        num_records (int): The number of dummy records to generate.

    Returns:
        pandas.DataFrame: A DataFrame containing the generated dummy data.
    """
    fake = Faker('en_US') # Initialize Faker with US locale for relevant data like SSN

    data = []
    for _ in range(num_records):
        record = {
            'id': fake.uuid4(),
            'first_name': fake.first_name(),
            'last_name': fake.last_name(),
            'email': fake.email(),
            'date_of_birth': fake.date_of_birth(minimum_age=18, maximum_age=90).strftime('%Y-%m-%d'),           
            
            'company': fake.company(),
            'city': fake.city(),
            
            'password': fake.password(length=12, special_chars=True, digits=True, upper_case=True, lower_case=True),
            'salary': fake.random_int(min=30000, max=150000),
            'random_string': fake.pystr(min_chars=10, max_chars=20)
        }
        data.append(record)

    df = pd.DataFrame(data)
    return df

def upload_to_gcs(bucket_name, source_file_name, destination_blob_name):
    """Uploads a file to the GCS bucket."""
    try:
        # Initialize a client
        storage_client = storage.Client()

        # Get the bucket
        bucket = storage_client.bucket(bucket_name)

        # Create a blob (object) and upload the file
        blob = bucket.blob(destination_blob_name)
        blob.upload_from_filename(source_file_name)

        print(f"File {source_file_name} uploaded to {destination_blob_name} in bucket {bucket_name}.")
    except Exception as e:
        print(f"Error uploading file to GCS: {e}")

if __name__ == "__main__":
    # --- Dummy Data Generation ---
    # Generate 5 records of dummy data
    print("Generating 5 records of dummy data:")
    dummy_df_small = generate_dummy_data(5)
    print(dummy_df_small)
    print("\n" + "="*50 + "\n")

    # Generate 20 records and save to a CSV file
    csv_file_name = 'dummy_data.csv'
    print(f"Generating 20 records and saving to '{csv_file_name}'...")
    dummy_df_large = generate_dummy_data(20)
    dummy_df_large.to_csv(csv_file_name, index=False)
    print(f"Dummy data saved to '{csv_file_name}'.")
    print(dummy_df_large.head())
    print("\n" + "="*50 + "\n")

    # --- GCS Upload ---
    gcs_bucket_name = 'bkt-emp-ind-data' # <<< IMPORTANT: Replace with your GCS bucket name
    gcs_destination_blob_name = 'dummy_data_generated.csv' # Name of the file in GCS

    print(f"Attempting to upload '{csv_file_name}' to GCS bucket '{gcs_bucket_name}'...")
    upload_to_gcs(gcs_bucket_name, csv_file_name, gcs_destination_blob_name)

    # Optional: Clean up the local CSV file after upload
    # if os.path.exists(csv_file_name):
    #     os.remove(csv_file_name)
    #     print(f"Local file '{csv_file_name}' removed.")
