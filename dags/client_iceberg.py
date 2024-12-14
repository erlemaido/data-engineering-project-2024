from pyiceberg.catalog import load_rest

CATALOG = load_rest(name="rest",
                     conf={
                         "uri": "http://iceberg_rest:8181/",
                         "s3.endpoint": "http://minio:9000",
                         "s3.access-key-id": "minioadmin",
                         "s3.secret-access-key": "minioadmin",
                     })


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


