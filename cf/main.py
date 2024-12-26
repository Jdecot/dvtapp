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

     # Créer un client Cloud Storage
     storage_client = storage.Client()

     # Récupérer le bucket
     bucket = storage_client.get_bucket(bucket_name)

     # Récupérer les informations sur le bucket
     project_id = storage_client.project
     location = bucket.location

     # Fixer l'ID du dataset en dur (vous pouvez le changer par une variable d'environnement si besoin)
     dataset_id = "ds_dvtapp"
     cloud_run_url = "https://dvtapp-service-809726561816.us-east1.run.app"
     
     # Construire les données à envoyer à Cloud Run
     data = {
          "file": f"gs://{bucket_name}/{file}",
          "project_id": project_id,
          "region": location,
          "dataset_id": dataset_id
     }
     print(data)
     # Envoyer une requête POST à Cloud Run
     response = requests.post(cloud_run_url, json=data)
     print(response.text)


# ******************** For Testing *************************
# event={
#      "name": "data.json",
#      "bucket": f"dvtapp_bucket_receiving_json_files",
# }
# context = {}
# hello_gcs(event, context)