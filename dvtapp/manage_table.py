from google.cloud import bigquery
import datetime
import time

def delete_all_tables(client, project, dataset_id):
    """Supprime toutes les tables d'un dataset BigQuery.

    Args:
        client: Un client BigQuery.
        project: Le nom du projet.
        dataset_id: L'ID du dataset.
    """

    dataset_ref = client.dataset(dataset_id)
    tables = client.list_tables(dataset_ref)

    for table in tables:
        table_id = table.table_id
        table_ref = dataset_ref.table(table_id)
        client.delete_table(table_ref)
        print(f"Table {table_id} supprimée.")
        

def create_table_name():
    format="%d%m%Y%H%M%S"
    maintenant = datetime.datetime.now()
    table_name = str("dvtapp_table_"+maintenant.strftime(format))
    return table_name

def create_table_if_not_exists(client, dataset_id, table_id, initial_schema=[]):
    """Creates a BigQuery table if it doesn't exist.

    Args:
        client: A BigQuery client.
        dataset_id: The ID of the dataset.
        table_id: The ID of the table.
        schema: The schema for the table.
    """

    dataset_ref = client.dataset(dataset_id)
    table_ref = dataset_ref.table(table_id)

    try:
        # Utilisez list_tables() pour vérifier l'existence de la table
        tables = client.list_tables(dataset=dataset_ref)
        table_exists = any(table.table_id == table_id for table in tables)

        if not table_exists:
            table = bigquery.Table(table_ref, schema=initial_schema)
            table = client.create_table(table)
            print(f"Created table : {dataset_id}.{table_id}")
    except Exception as e:
        print(f"Error creating table: {e}")


def insert_data_with_retry(client, dataset_id, table_id, data):
    max_retries = 10
    retry_delay = 1  # En secondes

    for attempt in range(max_retries):
        try:
            table = client.get_table(f"{dataset_id}.{table_id}") 
            errors = client.insert_rows(table, [data])
            if errors:
                raise Exception(f"Error during row insertion: {errors}")
            print(f"Row inserted: {data}")
            return
        except Exception as e:
            if "no such field" in str(e):
                print(f"Waiting for column propagation. Retry in {retry_delay} seconds...")
                time.sleep(retry_delay)
                retry_delay *= 2
            else:
                raise

def add_column_to_table(client, dataset_id, table_id, column_name, SchemaField):
    # print("add_column_to_table : ", column_name)
    table_ref = client.dataset(dataset_id).table(table_id)
    table = client.get_table(table_ref)

    hard_copy_schema = table.schema

    hard_copy_schema.append(SchemaField)

    table.schema = hard_copy_schema
    table = client.update_table(table, ["schema"])
    table = client.get_table(table_ref) 
    print("column added : ", column_name)


def compare_dict_to_schema(client, dataset_id, table_id, data_dict):

    print("compare_dict_to_schema")
    table_ref = client.dataset(dataset_id).table(table_id)
    table = client.get_table(table_ref)
    schema = table.schema

    missing_columns = []
    for key in data_dict.keys():
        found = False
        for field in schema:
            if field.name == key:
                found = True
                break
        if not found:
            missing_columns.append(key)
    print("missing columns : " + str(missing_columns))
    return missing_columns

