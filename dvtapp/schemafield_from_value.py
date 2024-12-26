import datetime
from google.cloud import bigquery
from google.cloud.bigquery import SchemaField


def get_field_type(value):
    """
    Identify the type of the value
    """
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
            return 'ARRAY<ANY>'
    elif isinstance(value, dict):
        return 'RECORD'
    else:
        return 'STRING'
    

def merge_types(type1, type2):
    """
    Merge two data types and return a type that is compatible with both.
    If the types are incompatible, the function promotes to a general compatible type.

    Parameters:
    - type1: str, the first type (e.g., "INTEGER", "FLOAT", "STRING").
    - type2: str, the second type (e.g., "INTEGER", "FLOAT", "STRING").

    Returns:
    - str: A type that is compatible with both inputs.
    """
    # If the types are identical, return the type
    if type1 == type2:
        return type1

    # Handle specific cases of compatibility
    compatible_types = {
        frozenset({"BOOLEAN", "INTEGER"}): "INTEGER",  # BOOLEAN + INTEGER -> INTEGER
        frozenset({"BOOLEAN", "FLOAT"}): "FLOAT",     # BOOLEAN + FLOAT -> FLOAT
        frozenset({"INTEGER", "FLOAT"}): "FLOAT",     # INTEGER + FLOAT -> FLOAT
        frozenset({"BOOLEAN", "STRING"}): "STRING",   # BOOLEAN + STRING -> STRING
        frozenset({"INTEGER", "STRING"}): "STRING",   # INTEGER + STRING -> STRING
        frozenset({"FLOAT", "STRING"}): "STRING",     # FLOAT + STRING -> STRING
        frozenset({"ARRAY", "STRING"}): "ARRAY",      # ARRAY + STRING -> ARRAY
        frozenset({"STRUCT", "STRING"}): "STRUCT",    # STRUCT + STRING -> STRUCT
    }

    # Check if the pair of types has a specific compatibility rule
    merged_type = compatible_types.get(frozenset({type1, type2}))
    if merged_type:
        return merged_type

    # Handle ARRAY types
    if "ARRAY" in {type1, type2}:
        return "ARRAY"

    # Handle STRUCT types
    if "STRUCT" in {type1, type2}:
        return "STRUCT"

    # Default case: promote to STRING for compatibility
    return "STRING"


def merge_schemafields(field1, field2):
    """
    Compare two SchemaField objects and return a new SchemaField that is compatible with both.
    If the two fields are incompatible, a new compatible SchemaField is generated.

    Parameters:
    - field1: SchemaField
    - field2: SchemaField

    Returns:
    - SchemaField: A SchemaField object compatible with both input fields.
    """
    if field1.name != field2.name:
        raise ValueError("Field names must match to be merged.")

    # Resolve the field type
    resolved_type = merge_types(field1.field_type, field2.field_type)

    # Resolve the mode (nullable, repeated, required)
    if field1.mode == "REPEATED" or field2.mode == "REPEATED":
        resolved_mode = "REPEATED"
    elif field1.mode == "NULLABLE" or field2.mode == "NULLABLE":
        resolved_mode = "NULLABLE"
    else:
        resolved_mode = "REQUIRED"

    # Handle nested fields for RECORD/STRUCT types
    if resolved_type == "RECORD":
        # Collect all nested fields from both SchemaFields
        nested_fields1 = {field.name: field for field in field1.fields}
        nested_fields2 = {field.name: field for field in field2.fields}

        # Merge nested fields recursively, handling missing fields in each schema
        all_nested_field_names = set(nested_fields1.keys()).union(nested_fields2.keys())
        
        # Merge each nested field or create it as a new SchemaField if missing
        merged_nested_fields = [
            merge_schemafields(
                nested_fields1.get(name, SchemaField(name, "STRING", "NULLABLE")),
                nested_fields2.get(name, SchemaField(name, "STRING", "NULLABLE"))
            )
            for name in all_nested_field_names
        ]
    else:
        # No nested fields if not a RECORD type
        merged_nested_fields = []

    # Return the merged SchemaField
    return SchemaField(
        name=field1.name,
        field_type=resolved_type,
        mode=resolved_mode,
        fields=merged_nested_fields
    )

def build_schema_field(field_name, field_type):
    return bigquery.SchemaField(field_name, field_type, "NULLABLE") 

def schemafield_from_value(key, value):
    """
    Generates a BigQuery SchemaField from the provided key and value.
    This function infers the schema based on the value's type. If the value is a 
    RECORD (a nested structure), it recursively constructs SchemaField objects 
    for its sub-fields. If the value is an ARRAY, it handles both simple arrays 
    and arrays of RECORDs.

    Parameters:
    - key (str): The name of the field to be used in the schema.
    - value (any): The value associated with the field. The type of this value 
                  determines the schema to be created.

    Returns:
    - bigquery.SchemaField: A SchemaField object representing the field in the 
                             BigQuery schema.

    Example:
    If the value is a nested dictionary (representing a RECORD):
        value = {'status': 'current', 'address': '123 Main Street'}
        schemafield_from_value('user_address', value)
        Returns a SchemaField for 'user_address' with type 'RECORD' and sub-fields 
        'status' and 'address'.
        
    If the value is an array of records:
        value = [{'status': 'current', 'address': '123 Main Street'}, {'status': 'previous', 'address': '456 Main Street'}]
        schemafield_from_value('addresses', value)
        Returns a SchemaField for 'addresses' with type 'ARRAY<RECORD>', and sub-fields 
        'status' and 'address'.
    """

    # Determine the field type of the value (e.g., STRING, INTEGER, RECORD, ARRAY)
    field_type = get_field_type(value)

    # Initialize an empty list to store nested fields (used for RECORD or ARRAY<RECORD>)
    schema = []
    if field_type == 'RECORD':

        # If the field is a RECORD (nested structure), recursively build the schema for sub-fields
        SchemaField = bigquery.SchemaField(
            key,
            field_type,
            fields= [ # Create SchemaFields for each sub-field of the RECORD
                build_schema_field(sub_key, get_field_type(sub_value)) 
                for sub_key, sub_value in value.items()
            ],
            mode="NULLABLE" # By default, set RECORD fields as NULLABLE
        )
        return SchemaField
    
    # If the field is an ARRAY, we need to determine the type of elements in the ARRAY
    elif field_type.startswith('ARRAY'):
        # Déterminer le type des éléments de l'array
        array_type = field_type[6:-1]  # Extract the type of elements (e.g., "INTEGER" from "ARRAY<INTEGER>")
        
        # If the elements in the array are RECORDs, create a RECORD schema for each element
        if array_type == 'RECORD':
            schema = [
                build_schema_field(sub_key, get_field_type(sub_value))
                for sub_key, sub_value in value[0].items()  # Use the first element of the array to define sub-fields
            ]
            # Create a SchemaField for the ARRAY of RECORDs
            SchemaField = bigquery.SchemaField(
                key,
                'RECORD',  # Type 'RECORD' for an ARRAY of RECORDs
                mode="REPEATED", # Indicate that this is a repeated (ARRAY) field
                fields=schema
            )
        else:
            # If the array elements are not RECORDs, just set the element type (e.g., INTEGER, STRING)
            SchemaField = bigquery.SchemaField(
                key,
                array_type,
                mode="REPEATED"  # Indicate that this is a repeated (ARRAY) field
            )
        return SchemaField
    
    # If the field is a simple type (e.g., STRING, INTEGER), create a SchemaField without nested fields
    else:
        SchemaField = bigquery.SchemaField(key,field_type)
        return SchemaField
