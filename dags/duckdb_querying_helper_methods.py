from pyiceberg.catalog import load_rest
import duckdb

catalog = load_rest(name="rest",
                    conf={
                        "uri": "http://localhost:8181/",
                        "s3.endpoint": "http://localhost:9000",
                        "s3.access-key-id": "minioadmin",
                        "s3.secret-access-key": "minioadmin",
                    },
                    )
namespace = "staging"
table_name = "tax_data"


def select_table():
    return print(catalog.load_table(f"{namespace}.{table_name}").scan().to_pandas())


def drop_table():
    catalog.drop_table(f"{namespace}.{table_name}")


def connect_duckdb_table():
    conn = duckdb.connect("../data/data.duckdb")
    conn.sql("INSTALL httpfs")
    conn.sql("LOAD httpfs")
    conn.sql("""
    SET s3_region='us-east-1';
    SET s3_url_style='path';
    SET s3_endpoint='localhost:9001';
    SET s3_access_key_id='minioadmin' ;
    SET s3_secret_access_key='minioadmin';
    SET s3_use_ssl=false;
    """)

    #print(conn.sql("select * from fy_report_dim"))
    #print(conn.sql("select * from entity_dim where name is not NULL and legal_form is not NULL"))
    #print(conn.sql("select * from date_dim"))
    print(conn.sql("select count(1) from financial_performance_fact"))
    print(conn.sql("select * from financial_performance_fact"))
    #print(conn.sql("select * from tax_fact"))



if __name__ == '__main__':
    connect_duckdb_table()
