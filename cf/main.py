import requests
from google.cloud import storage

def hello_gcs(event, context):
     """Triggered by a change in a cloud storage bucket.
     Args:
          event (dict): Event payload.
          context (google.cloud.functions.Context): Metadata for the event.
     """
     file = event['name']
     bucket_name = event['bucket']

     # Create cloud storage client
     storage_client = storage.Client()

     # Get bucket involved in the event
     bucket = storage_client.get_bucket(bucket_name)

     # Get infos from bucket
     project_id = storage_client.project
     location = bucket.location

     # Set parameters
     dataset_id = "ds_dvtapp"
     cloud_run_url = "https://dvtapp-service-809726561816.us-east1.run.app"
     
     # Build data to send to cloud run
     data = {
          "file": f"gs://{bucket_name}/{file}",
          "project_id": project_id,
          "region": location,
          "dataset_id": dataset_id
     }
     print(data)
     # Send the request
     response = requests.post(cloud_run_url, json=data)
     print(response.text)


# ******************** For Testing *************************
# event={
#      "name": "data.json",
#      "bucket": f"dvtapp_bucket_receiving_json_files",
# }
# context = {}
# hello_gcs(event, context)