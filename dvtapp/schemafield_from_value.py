import datetime
from google.cloud import bigquery



def get_field_type(value):
    if isinstance(value, int):
        return 'INTEGER'
    elif isinstance(value, float):
        return 'FLOAT'
    elif isinstance(value, str):
        try:
            datetime.datetime.fromisoformat(value)
            return 'TIMESTAMP'
        except ValueError:
            return 'STRING'
    elif isinstance(value, bool):
        return 'BOOLEAN'
    elif isinstance(value, list):
        if all(isinstance(item, int) for item in value):
            return 'ARRAY<INTEGER>'
        elif all(isinstance(item, float) for item in value):
            return 'ARRAY<FLOAT>'
        elif all(isinstance(item, str) for item in value):
            return 'ARRAY<STRING>'
        elif all(isinstance(item, bool) for item in value):
            return 'ARRAY<BOOLEAN>'
        elif all(isinstance(item, dict) for item in value):
            return 'ARRAY<RECORD>'
        else:
            return 'ARRAY'
    elif isinstance(value, dict):
        return 'RECORD'
    else:
        return 'STRING'
    

def build_schema_field(field_name, field_type):
    return bigquery.SchemaField(field_name, field_type, "NULLABLE") 

def schemafield_from_value(key, value):
    field_type = get_field_type(value)

    schema = []
    if field_type == 'RECORD':

        SchemaField = bigquery.SchemaField(
            key,
            field_type,
            fields= [
                build_schema_field(sub_key, get_field_type(sub_value)) 
                for sub_key, sub_value in value.items()
            ],
            mode="NULLABLE"
        )
        return SchemaField
    elif field_type.startswith('ARRAY'):
        # Déterminer le type des éléments de l'array
        array_type = field_type[6:-1]  # Extraire le type des éléments (e.g., "INTEGER" de "ARRAY<INTEGER>")
        SchemaField = bigquery.SchemaField(
            key,
            array_type,
            mode="REPEATED"
        )
        return SchemaField
    else:
        SchemaField = bigquery.SchemaField(key,field_type)
        return SchemaField

    



"""
----------------------------------------------------------------------------------------------------------------
--------------------------------------------------- Test schemafield_from_value --------------------------------
----------------------------------------------------------------------------------------------------------------
"""

# ts = "2020-06-18T10:44:12"
# chiffre = 12
# phrase = "bonjour"
# started = {"pid":45678}
# sessions_ids = [123, 456]
# logged_in = {"username":"foo"}
# addresses = [{"status":"current","address":"123 First Avenue","city":"Seattle","state":"WA","zip":"11111","numberOfYears":"1"},
#              {"status":"previous","address":"456 Main Street","city":"Portland","state":"OR","zip":"22222","numberOfYears":"5"}
#             ]

# print(schemafield_from_value("ts", ts))
# print(schemafield_from_value("chiffre", chiffre))
# print(schemafield_from_value("phrase", phrase))
# print(schemafield_from_value("sessions_ids", sessions_ids))
# print(schemafield_from_value("started", started))
# print(schemafield_from_value("logged_in", logged_in))
# print(schemafield_from_value("addresses", addresses))


"""
----------------------------------------------------------------------------------------------------------------
--------------------------------------------------- Test add_column_to_table -----------------------------------
----------------------------------------------------------------------------------------------------------------
"""
# def add_column_to_table(client, dataset_id, table_id, column_name, SchemaField):
#     print("add_column_to_table : ", column_name)
#     table_ref = client.dataset(dataset_id).table(table_id)
#     table = client.get_table(table_ref)

#     hard_copy_schema = table.schema

#     hard_copy_schema.append(SchemaField)

#     table.schema = hard_copy_schema
#     table = client.update_table(table, ["schema"])
#     print("column added : ", column_name)


# import os
# os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'C:/jsongcp/dvtapp-198e02b1453a.json'
# bq_client = bigquery.Client(project="dvtapp")
# project_id = "dvtapp"
# region = "us-east1"
# dataset_id = "ds_dvtapp"
# table_id = "test_table_17"


# key = "started"
# value = {"pid":45678}
# SchemaField = schemafield_from_value(key, value)
# print("SchemaField : ", SchemaField)

# add_column_to_table(bq_client, dataset_id, table_id, key, SchemaField)


"""
----------------------------------------------------------------------------------------------------------------
--------------------------------------------------- Improve -----------------------------------------------
----------------------------------------------------------------------------------------------------------------
"""

# Create new field REPEATED "telephones"

# JSON
# {
#   "telephones": [
#     {"numero": "1234567890"},
#     {"numero": "9876543210"}
#   ]
# }

# BigQuery version
# telephone_field = bigquery.SchemaField(
#     "telephones",
#     "ARRAY",
#     fields=[
#         bigquery.SchemaField("numero", "STRING")
#     ],
#     mode="REPEATED"
# )