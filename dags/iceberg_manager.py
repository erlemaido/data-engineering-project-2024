from pyiceberg.catalog import load_rest
from minio import Minio
from minio.error import S3Error

BUCKET = "bucket"

CATALOG = load_rest(name="rest",
                     conf={
                         "uri": "http://iceberg_rest:8181/",
                         "s3.endpoint": "http://minio:9000",
                         "s3.access-key-id": "minioadmin",
                         "s3.secret-access-key": "minioadmin",
                     })

MINIO_CLIENT = Minio(
        "minio:9000",
        access_key="minioadmin",
        secret_key="minioadmin",
        secure=False
    )

def create_namespace_if_not_exists(namespace):
    try:
        CATALOG.create_namespace_if_not_exists(namespace)
    except Exception as e:
        print(f"Namespace creation failed: {e}")

def load_table(namespace, table_name):
    return CATALOG.load_table(f"{namespace}.{table_name}")

def create_table_if_not_exists(namespace, table_name, schema, arrow_table):
    table = CATALOG.create_table_if_not_exists(
        identifier=f"{namespace}.{table_name}",
        schema=schema,
    )
    table.append(arrow_table)

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

    except S3Error as e:
        print("S3Error: ", e)
    except Exception as e:
        print("Error: ", e)
