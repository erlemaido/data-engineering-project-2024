from minio import Minio
from minio.error import S3Error


def upload_to_minio(filepath, filename):
    minio_endpoint = "minio:9000"  # Replace with your MinIO server's URL
    access_key = "minioadmin"
    secret_key = "minioadmin"
    bucket_name = "bucket"
    file_path = filepath  # Path to your local CSV file
    object_name = filename  # The name you want to assign to the file in the bucket

    client = Minio(
        minio_endpoint,
        access_key=access_key,
        secret_key=secret_key,
        secure=False
    )

    try:
        found = client.bucket_exists(bucket_name)
        if not found:
            client.make_bucket(bucket_name)
        else:
            print(f"Bucket '{bucket_name}' already exists.")

        client.fput_object(bucket_name, object_name, file_path)
        print(f"'{file_path}' is successfully uploaded as '{object_name}' to bucket '{bucket_name}'.")

    except S3Error as e:
        print("S3Error: ", e)
    except Exception as e:
        print("Error: ", e)
