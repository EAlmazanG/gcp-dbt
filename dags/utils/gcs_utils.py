import requests
from google.cloud import storage

def download_and_upload_to_gcs_batch(url, local_path, bucket_name, blob_name):
    response = requests.get(url)
    with open(local_path, "wb") as f:
        f.write(response.content)
    client = storage.Client()
    bucket = client.get_bucket(bucket_name)
    blob = bucket.blob(blob_name)
    blob.upload_from_filename(local_path)
