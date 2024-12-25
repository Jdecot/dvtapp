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
