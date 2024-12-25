import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions
import json
from google.cloud import bigquery
import manage_table
import schemafield_from_value
from flask import Flask, request, jsonify

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


def run_pipeline(filename):
    print("fct run pipeline, filename : ", filename)
    manage_table.delete_all_tables(bq_client, project_id, dataset_id)   
    print("Created table_id : ",table_id) 
    manage_table.create_table_if_not_exists(bq_client, dataset_id, table_id, initial_schema=[])
    
    with beam.Pipeline(options=PipelineOptions()) as pipeline:
        (
            pipeline
            | "Lire le fichier JSON" >> beam.io.ReadFromText(filename)
            | 'Process JSON' >> beam.Map(process_json_line)
        )


def trigger_pipeline(filename):
    print("fct trigger pipeline, filename : ", filename)

    global project_id, region, dataset_id, bq_client, table_id

    project_id = "dvtapp"
    region = "us-east1"

    # Paramètres BigQuery
    dataset_id = "ds_dvtapp"

    options = PipelineOptions(
        runner='DirectRunner',
        project=project_id,
        region=region,
    )

    bq_client = bigquery.Client(project="dvtapp")
    table_id = manage_table.create_table_name()
    # Execute the pipeline
    run_pipeline(filename)
    #=return 'Pipeline triggered successfully!', 200


app = Flask(__name__)

@app.route('/', methods=['GET', 'POST'])  # Allow both GET and POST requests
def main():
    data = request.get_json()
    filename = data.get('file')

    if filename:
        print(f'Le nom de fichier est : {filename}')
        print("Triggering pipeline...")
        trigger_pipeline(filename)
        return jsonify({'message': 'Pipeline triggered successfully!'})  # Return a success message
    else:
        return jsonify({'error': 'Missing filename parameter'}), 400  # Return error for missing filename


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8080) 