import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions
import json
from google.cloud import bigquery
import manage_table
import schemafield_from_value
from flask import Flask, request, jsonify
import os
import signal

def process_json_line(element):
    """
    This function processes a single JSON data line, compares its schema with the BigQuery table schema,
    adds missing columns if necessary, and inserts the data into BigQuery.
    """
    data = json.loads(element)
    print("------------------- New Row ----------------------")
    print("data : ", data)

    # Compare the data's keys with the schema of the BigQuery table
    missing_columns = manage_table.compare_dict_to_schema(bq_client, dataset_id, table_id, data)
    print("missing_columns : ", missing_columns)

    # For each missing column, add it to the BigQuery table
    for column in missing_columns:
        SchemaField = schemafield_from_value.schemafield_from_value(column, data[column])
        manage_table.add_column_to_table(bq_client, dataset_id, table_id, column, SchemaField)

    # Insert the data into BigQuery with retry logic in case of failure
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
    
    # Start the Apache Beam pipeline to process the JSON file
    # Read the JSON file from Cloud Storage
    # Process each JSON line
    with beam.Pipeline(options=PipelineOptions()) as pipeline:
        (
            pipeline
            | "Lire le fichier JSON" >> beam.io.ReadFromText(filename)
            | 'Process JSON' >> beam.Map(process_json_line)
        )
    return 
    
 
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
        print("return message")
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