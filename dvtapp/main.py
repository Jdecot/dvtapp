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
    data = json.loads(element)
    print("------------------- New Row ----------------------")
    print("data : ", data)
    missing_columns = manage_table.compare_dict_to_schema(bq_client, dataset_id, table_id, data)
    print("missing_columns : ", missing_columns)

    for column in missing_columns:
        SchemaField = schemafield_from_value.schemafield_from_value(column, data[column])
        manage_table.add_column_to_table(bq_client, dataset_id, table_id, column, SchemaField)

    manage_table.insert_data_with_retry(bq_client, dataset_id, table_id, data)


def trigger_pipeline(filename):
    print("fct trigger pipeline, filename : ", filename)

    # Define Parameters
    global project_id, region, dataset_id, bq_client, table_id
    project_id = "dvtapp"
    region = "us-east1"
    dataset_id = "ds_dvtapp"
    bq_client = bigquery.Client(project=project_id)
    table_id = manage_table.create_table_name() 

    # Delete tables in dataset and create a new one
    manage_table.delete_all_tables(bq_client, project_id, dataset_id)  
    manage_table.create_table_if_not_exists(bq_client, dataset_id, table_id, initial_schema=[])
    
    with beam.Pipeline(options=PipelineOptions()) as pipeline:
        (
            pipeline
            | "Lire le fichier JSON" >> beam.io.ReadFromText(filename)
            | 'Process JSON' >> beam.Map(process_json_line)
        )
    return 
    
 
app = Flask(__name__)

@app.route('/', methods=['GET', 'POST'])  # Allow both GET and POST requests
def main():
    data = request.get_json()
    filename = data.get('file')

    if filename:
        print(f'Filename received from the Cloud Function : {filename}')
        print("Triggering pipeline...")
        trigger_pipeline(filename)
        response = "Pipeline finished successfully!"
        print(response)
        # Marquer pour arrêter le serveur après la réponse
        request.environ['shutdown'] = True
        print("return message")
        return jsonify({'message': response}), 200
    else:
        return jsonify({'error': 'Missing filename parameter'}), 400


@app.after_request
def shutdown_after_request(response):
    """Arrête le serveur après avoir traité la réponse."""
    if request.environ.get('shutdown'):
        os.kill(os.getpid(), signal.SIGINT)
    return response


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8080) 