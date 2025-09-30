from google.cloud import storage

def list_buckets(project_id: str):
    """
    Lists all buckets in the given Google Cloud project.
    """
    # Initialize client
    client = storage.Client(project=project_id)

    print(f"Buckets in project {project_id}:")
    for bucket in client.list_buckets():
        print(bucket.name)
    blobs = client.list_blobs("gcs-ecommerce-data")
    for blob in blobs:
        print(blob.name)

if __name__ == "__main__":
    project_id = "hadoop-and-yarn"  # Replace with your project ID
    list_buckets(project_id)

