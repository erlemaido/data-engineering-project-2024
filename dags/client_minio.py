from minio import Minio, S3Error

BUCKET = "bucket"

MINIO_CLIENT = Minio(
        "minio:9000",
        access_key="minioadmin",
        secret_key="minioadmin",
        secure=False
    )

def upload_to_minio(filepath, filename):
    try:
        # Check if bucket exists; create it if not
        if not MINIO_CLIENT.bucket_exists(BUCKET):
            MINIO_CLIENT.make_bucket(BUCKET)
        else:
            print(f"Bucket '{BUCKET}' already exists.")

        # Upload the file
        MINIO_CLIENT.fput_object(BUCKET, filename, filepath)
        print(f"'{filepath}' is successfully uploaded as '{filename}' to bucket '{BUCKET}'.")

        return f"s3://{BUCKET}/{filename}"

    except S3Error as e:
        print("S3Error: ", e)
    except Exception as e:
        print("Error: ", e)