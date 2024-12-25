import requests

def hello_gcs(event, context):
    """Triggered by a change in a cloud storage bucket.
    Args:
         event (dict): Event payload.
         context (google.cloud.functions.Context): Metadata for the event.
    """
    file = event['name']
    bucket = event['bucket']

    # Construire l'URL de l'endpoint Cloud Run
    cloud_run_url = "https://dvtapp-service-809726561816.us-east1.run.app"

    # Construire les données à envoyer à Cloud Run
    data = {"file": f"gs://{bucket}/{file}"}

    # Envoyer une requête POST à Cloud Run
    response = requests.post(cloud_run_url, json=data)
    print(response.text)