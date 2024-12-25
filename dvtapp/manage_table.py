from google.cloud import bigquery
import datetime
import time

def delete_all_tables(client, project, dataset_id):
    """
    Deletes all tables in a specified BigQuery dataset.

    Args:
        client (bigquery.Client): A BigQuery client instance.
        project (str): The Google Cloud project ID.
        dataset_id (str): The ID of the dataset to clean.
    """
    # Get the list of all table present in the dataset
    dataset_ref = client.dataset(dataset_id)
    tables = client.list_tables(dataset_ref)

    # Remove all tables from the dataset
    for table in tables:
        table_id = table.table_id
        table_ref = dataset_ref.table(table_id)
        client.delete_table(table_ref)
        print(f"Table {table_id} supprimée.")
        

def create_table_name():
    """
    Generates a unique table name based on the current timestamp.

    Returns:
        str: A unique table name in the format `dvtapp_table_<timestamp>`.
    """
    format="%d%m%Y%H%M%S"
    now = datetime.datetime.now()
    table_name = str("dvtapp_table_"+now.strftime(format))
    return table_name


def create_table_if_not_exists(client, dataset_id, table_id, initial_schema=[]):
    """
    Creates a BigQuery table if it does not already exist.

    Args:
        client (bigquery.Client): A BigQuery client instance.
        dataset_id (str): The ID of the dataset where the table should be created.
        table_id (str): The desired ID of the table.
        initial_schema (list, optional): A list of BigQuery schema fields. Defaults to an empty schema.
    """
    # Identify the table and the dataset
    dataset_ref = client.dataset(dataset_id)
    table_ref = dataset_ref.table(table_id)

    try:
        # Check if a table already exists with the same name
        tables = client.list_tables(dataset=dataset_ref)
        table_exists = any(table.table_id == table_id for table in tables)

        # Create the new table to load the data
        if not table_exists:
            table = bigquery.Table(table_ref, schema=initial_schema)
            table = client.create_table(table)
            print(f"Created table : {dataset_id}.{table_id}")
    except Exception as e:
        print(f"Error creating table: {e}")


def insert_data_with_retry(client, dataset_id, table_id, data):
    """
    Inserts a row into a BigQuery table, retry if necessary.

    Args:
        client (bigquery.Client): A BigQuery client instance.
        dataset_id (str): The ID of the dataset containing the table.
        table_id (str): The ID of the table to insert data into.
        data (dict): The data row to insert.

    Raises:
        Exception: If the insertion fails after all retries.
    """
    max_retries = 10
    retry_delay = 1  # Initial delay in seconds

    for attempt in range(max_retries):
        # Try to insert row in the table
        try:
            table = client.get_table(f"{dataset_id}.{table_id}") 
            errors = client.insert_rows(table, [data])
            if errors:
                raise Exception(f"Error during row insertion: {errors}")
            print(f"Row inserted: {data}")
            return
        except Exception as e:
            # If a column doesn't exists, try again later, giving time to the new column to get created
            if "no such field" in str(e):
                print(f"Waiting for column propagation. Retry in {retry_delay} seconds...")
                time.sleep(retry_delay)
                retry_delay *= 2
            else:
                raise

def add_column_to_table(client, dataset_id, table_id, column_name, SchemaField):
    """
    Adds a new column to an existing BigQuery table.

    Args:
        client (bigquery.Client): A BigQuery client instance.
        dataset_id (str): The ID of the dataset containing the table.
        table_id (str): The ID of the table to modify.
        column_name (str): The name of the column to add.
        SchemaField (bigquery.SchemaField): The schema field definition for the new column.
    """

    table_ref = client.dataset(dataset_id).table(table_id)
    table = client.get_table(table_ref)

    # Add a new column to the current table's schema
    hard_copy_schema = table.schema
    hard_copy_schema.append(SchemaField)

    # Update the table's schema with the new one
    table.schema = hard_copy_schema
    table = client.update_table(table, ["schema"])
    table = client.get_table(table_ref) 
    print("Column added : ", column_name)


def compare_dict_to_schema(client, dataset_id, table_id, data_dict):
    """
    Compares a dictionary of data against a BigQuery table schema to find missing columns.

    Args:
        client (bigquery.Client): A BigQuery client instance.
        dataset_id (str): The ID of the dataset containing the table.
        table_id (str): The ID of the table to compare against.
        data_dict (dict): The data dictionary to validate.

    Returns:
        list: A list of missing column names.
    """
    print("Comparing data dictionary to schema...")
    table_ref = client.dataset(dataset_id).table(table_id)
    table = client.get_table(table_ref)
    schema = table.schema

    # Check if a column is present in the JSON data and is missing in the table's schema
    missing_columns = [key for key in data_dict.keys() if key not in {field.name for field in schema}]
    
    print("Missing columns : " + str(missing_columns))
    return missing_columns

