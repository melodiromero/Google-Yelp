import os
from google.cloud import storage
import pandas as pd
import numpy as np

bucket_nombre = 'yelp_gmaps'
ruta = 'Google Maps/reviews-estados/'

# Create a client
client = storage.Client()

# Get the bucket
bucket = client.get_bucket(bucket_nombre)

# List objects with the specified prefix
blobs = bucket.list_blobs(prefix=ruta)

# Initialize an empty DataFrame
joined_df = pd.DataFrame()
# Read and union all DataFrames
for blob in blobs:
    # Get the JSON file path
    json_file_path = f"gs://{bucket_nombre}/{blob.name}"

    # Read the JSON file into a Pandas DataFrame
    df = pd.read_json(json_file_path)

    # Append the DataFrame to the joined DataFrame
    joined_df = joined_df.append(df)

# Write the result to a Parquet file
output_parquet_path = f"gs://{bucket_nombre}/yelp_gmaps/Google Maps/reviews_unificados.parquet"

joined_df.to_parquet(output_parquet_path)


