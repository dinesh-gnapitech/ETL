import pyodbc
from pyspark.sql import SparkSession
import pandas as pd

# Configure MSSQL connection
mssql_config = {
    "server": "your_server_name",
    "database": "your_database_name",
    "username": "your_username",
    "password": "your_password",
}

# Initialize SparkSession
spark = SparkSession.builder.appName("MSSQLTableRecordCount").getOrCreate()

# Fetch all table names from MSSQL database
def get_table_names():
    connection_string = f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={mssql_config['server']};DATABASE={mssql_config['database']};UID={mssql_config['username']};PWD={mssql_config['password']}"
    conn = pyodbc.connect(connection_string)
    cursor = conn.cursor()
    cursor.execute("SELECT TABLE_SCHEMA, TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE'")
    tables = cursor.fetchall()
    conn.close()
    return [(row.TABLE_SCHEMA, row.TABLE_NAME) for row in tables]

# Count records for each table using PySpark
def count_records(spark, tables):
    record_counts = []
    for schema, table in tables:
        table_full_name = f"{schema}.{table}"
        try:
            df = spark.read \
                .format("jdbc") \
                .option("url", f"jdbc:sqlserver://{mssql_config['server']};databaseName={mssql_config['database']}") \
                .option("dbtable", table_full_name) \
                .option("user", mssql_config['username']) \
                .option("password", mssql_config['password']) \
                .load()
            record_count = df.count()
            record_counts.append({"Schema": schema, "Table": table, "RecordCount": record_count})
            print(f"Counted {record_count} records for table: {table_full_name}")
        except Exception as e:
            print(f"Error counting records for table {table_full_name}: {e}")
            record_counts.append({"Schema": schema, "Table": table, "RecordCount": "Error"})
    return record_counts

# Export results to Excel
def export_to_excel(record_counts, output_file):
    df = pd.DataFrame(record_counts)
    df.to_excel(output_file, index=False)
    print(f"Record counts exported to {output_file}")

# Main script
if __name__ == "__main__":
    tables = get_table_names()
    print(f"Found {len(tables)} tables in the database.")

    record_counts = count_records(spark, tables)

    output_file = "table_record_counts.xlsx"
    export_to_excel(record_counts, output_file)

    spark.stop()
