import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions
import json
from google.cloud import bigquery
import manage_table
import schemafield_from_value_v2
from flask import Flask, request, jsonify
import os
import signal



def process_for_columns_with_types(element):
    """
    Collect unique columns and their types present in the data.
    """
    data = json.loads(element)

    # For each columns in the JSON row of the data file
    for key, value in data.items():
        # For a column, we use the value to identify the schemafield
        schemafield_detected = schemafield_from_value_v2.schemafield_from_value(key, value)

        # If the currently studied key is already in the column list that we are preparing to create the table
        if key in all_columns:
            # So we check if for this row, the schematype is the same or not, if it's different, we take the more suitable one
            if all_columns[key] != schemafield_detected :
                new_schema_field = schemafield_from_value_v2.merge_schemafields(all_columns[key], schemafield_detected)
                print(f"Modify Column {key} : {schemafield_detected} in row | {all_columns[key]} in all_columns ==> {new_schema_field}")
                all_columns[key] = new_schema_field

        # If not, we create a new column
        else:
            all_columns[key] = schemafield_detected
            print(f"New Column {key} (value = {value}) ==> {schemafield_detected}")

    return data


def create_missing_columns(bq_client, dataset_id, table_id, all_columns, example_data):
    """
    Add all missing columns to BigQuery table.
    """
    # Get existing columns in the BigQuery table schema
    missing_columns = manage_table.compare_dict_to_schema(bq_client, dataset_id, table_id, example_data)
    
    # Add missing columns to BigQuery
    for column in missing_columns:
        SchemaField = schemafield_from_value_v2(column, example_data[column])
        manage_table.add_column_to_table(bq_client, dataset_id, table_id, column, SchemaField)


def process_for_loading(element):
    """
    Insert data into BigQuery.
    """
    data = json.loads(element)
    manage_table.insert_data_with_retry(bq_client, dataset_id, table_id, data)


def trigger_pipeline(filename):
    """
    This function initializes the BigQuery client, creates or deletes tables as needed,
    and starts the Apache Beam pipeline to process the JSON file.
    """

    # Define BigQuery client and the table to create his name 
    global bq_client, table_id
    bq_client = bigquery.Client(project=project_id)
    table_id = manage_table.create_table_name() 

    # Delete all existing tables in the dataset and create a new one
    manage_table.delete_all_tables(bq_client, project_id, dataset_id)  
    manage_table.create_table_if_not_exists(bq_client, dataset_id, table_id, initial_schema=[])
    
    # Shared set to collect all unique columns
    global all_columns
    all_columns = {}
    
    # First pass: identify and create all missing columns
    with beam.Pipeline(options=PipelineOptions()) as pipeline:
        example_data = (
            pipeline
            | "Read JSON for Columns" >> beam.io.ReadFromText(filename)
            | "Collect Columns" >> beam.FlatMap(lambda x: process_for_columns_with_types(x))
        )
    
    # Use example data to create all missing columns
    for column in all_columns:
        manage_table.add_column_to_table(bq_client, dataset_id, table_id, column, all_columns[column])

    # # Second pass: load data into BigQuery
    with beam.Pipeline(options=PipelineOptions()) as pipeline:
        (
            pipeline
            | "Read JSON for Loading" >> beam.io.ReadFromText(filename)
            | "Load Data" >> beam.Map(process_for_loading)
        )
    
 
# Flask web application to handle requests
app = Flask(__name__)

@app.route('/', methods=['GET', 'POST'])  # Allow both GET and POST requests
def main():
    """
    This function handles incoming requests from the Cloud Function, 
    retrieves the relevant parameters, and triggers the pipeline to process the data file in GCS.
    """
    data = request.get_json()

    # Extract parameters from the Cloud Function message
    global project_id, region, dataset_id
    project_id = data.get('project_id')
    region = data.get('region')
    dataset_id = data.get('dataset_id')
    filename = data.get('file')

    # If a filename is provided, trigger the pipeline
    if filename:
        print(f'Data received from the Cloud Function : file : {filename}, project_id : {project_id}, region : {region}, dataset_id : {dataset_id}')
        print("Triggering pipeline...")
        trigger_pipeline(filename)
        response = "Pipeline finished successfully!"
        print(response)

        # Shutdown the server after the response is sent
        request.environ['shutdown'] = True
        print("Return message to sender")
        return jsonify({'message': response}), 200
    else:
        # If no filename is provided, return an error response
        return jsonify({'error': 'Missing filename parameter'}), 400


@app.after_request
def shutdown_after_request(response):
    """
    This function ensures the server is stopped after handling the request.
    """
    if request.environ.get('shutdown'):
        os.kill(os.getpid(), signal.SIGINT)
    return response


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8080) 