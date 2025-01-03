# Description of the solution

Get a look to the Google Slides ! 


# Steps to test and deploy

### 1 - Test in local
1.1/ Start Flask server : 
    poetry run python dvtapp/main.py --runner=DirectRunner --project=dvtapp

1.2/ Send request to Flask server when it runs : 
    curl -X POST -H "Content-Type: application/json" -d "{\"file\": \"gs://dvtapp_bucket_receiving_json_files/data.json\", \"project_id\": \"dvtapp\", \"region\": \"us-east1\", \"dataset_id\": \"ds_dvtapp\"}" http://127.0.0.1:8080


### 2 - Build Docker image
docker build -t dvtapp-image-9 .


### 3 - Test Docker image
Test image with Flask : 
    docker run -p 8080:8080 -v C:\jsongcp:/app/config -e GOOGLE_APPLICATION_CREDENTIALS=/app/config/dvtapp-198e02b1453a.json dvtapp-image-9 python dvtapp/main.py
    curl -X POST -H "Content-Type: application/json" -d "{\"filename\": \"gs://dvtapp_bucket_receiving_json_files/data.json\"}" http://localhost:8080

See image content :
    docker run -it dvtapp-image-9 bash

Display logs in real time :
    docker exec -it <container_id> tail -f /app/app.log 
    docker logs -f <container_id>

In case of problem, stop and remove running containers :
    list containers : docker ps
    stop all : docker stop $(docker ps -a -q)
    remove all : docker rm $(docker ps -a -q)
    list again : docker ps


### 4 - Send image to GCR
docker tag dvtapp-image-9 northamerica-northeast1-docker.pkg.dev/dvtapp/dvtappdepot/dvtapp-image-9
docker push northamerica-northeast1-docker.pkg.dev/dvtapp/dvtappdepot/dvtapp-image-9


### 5 - Check if image is present in GCR
gcloud container images list-tags northamerica-northeast1-docker.pkg.dev/dvtapp/dvtappdepot/dvtapp-image-9


### 6 - Deploy image with Cloud Run
Deploy :
gcloud run deploy dvtapp-service --image=northamerica-northeast1-docker.pkg.dev/dvtapp/dvtappdepot/dvtapp-image-9 --region=us-east1

Trigger the Cloud Run Service from local : 
curl -X POST "https://dvtapp-service-809726561816.us-east1.run.app" -H "Content-Type: application/json" -d "{\"file\": \"gs://dvtapp_bucket_receiving_json_files/data.json\"}"


### 7 - Deploy the Cloud Function 
gcloud functions deploy hello_gcs --runtime python310 --trigger-resource dvtapp_bucket_receiving_json_files --trigger-event google.storage.object.finalize --region us-east1 --source "C:/Users/Julie/OneDrive/Documents/devoteamapp/app/cf/"

Test in local :
poetry run python cf/main.py
poetry run python cf/main.py --file "gs://dvtapp_bucket_receiving_json_files/data.json" --project_id "dvtapp" --region "us-east1" --dataset_id "ds_dvtapp"



# Not important / others, infos and commands

## Infos
C:\jsongcp\dvtapp-198e02b1453a.json
gs://dvtapp_bucket_receiving_json_files
Service url : https://dvtapp-service-809726561816.us-east1.run.app

## Commandes Docker
Ajouter variable env dans le dockerfile
ENV GOOGLE_APPLICATION_CREDENTIALS=/path/vers/votre/fichier/service-account.json

Lancer un conteneur en mode interactif
docker run -it mon-image /bin/bash

### Utiliser Docker
Construire l'image : docker build -t mon-image1 .
Tester l'image localement : docker run -it mon-pipeline
Tester un script dans l'image : docker run mon-image1 python main.py

Docker run image : 
    Run simple
        docker run mon-image1 python dvtapp/pipeline_8.py
    Avec variable d'environnement
        docker run -e GOOGLE_APPLICATION_CREDENTIALS=C:\jsongcp\dvtapp-198e02b1453a.json mon-image1 python dvtapp/pipeline_8.py
    Avec un volume pour que le dossier avec le json SA soit accessible
        docker run -v C:\jsongcp:/app/config -e GOOGLE_APPLICATION_CREDENTIALS=/app/config/dvtapp-198e02b1453a.json mon-image1 python dvtapp/pipeline_8.py

### Clear cache Docker
docker image prune -a -f 
docker container prune -f
docker network prune -f
docker volume prune -f 

### Connect Docker
gcloud auth configure-docker northamerica-northeast1-docker.pkg.dev

se connecter à GCR avec un SA : 
docker login -u _json_key -p $(gcloud auth print-access-token) https://gcr.io
gcloud auth print-access-token | docker login -u _json_key -p- https://gcr.io

Accéder au container :  docker exec -it dvtapp-image-9 bash 
Checker les ports qui écoutent : netstat -tulnp | grep LISTEN | grep 8080 


## Se connecter à GCP depuis le terminal
se loguer : gcloud auth login
Activer le SA : gcloud auth activate-service-account --key-file=C:\jsongcp\dvtapp-198e02b1453a.json
voir le compte actif actuel : gcloud auth list
Définir le compte à utiliser : gcloud config set account `ACCOUNT`

## Poetry Comands
poetry run python dvtapp/pipeline_8.py --runner=DirectRunner --project=dvtapp
poetry run run_pipeline -e GOOGLE_APPLICATION_CREDENTIALS=/path/to/your/file.json
poetry run python dvtapp/pipeline_8.py --runner=DirectRunner --project=dvtapp GOOGLE_APPLICATION_CREDENTIALS=C:/jsongcp/dvtapp-198e02b1453a.json

## Test Cloud Function
curl -X POST http://localhost:5001/<votre-projet>/us-central1/helloWorld -d '{"message": "Hello from local test"}'

## Commande cloud run
gcloud run deploy my-service --image=gcr.io/dvtapp/dvtapp-image --region=us-east1

Autoriser requête non authentifié :
gcloud run services add-iam-policy-binding dvtapp-service --member="allUsers" --role="roles/run.invoker" --region us-east1

# Debug
Bucket : gs://dvtapp_bucket_receiving_json_files

gsutil ls -L -b gs://dvtapp_bucket_receiving_json_files : US-EAST1

gcloud services enable cloudfunctions.googleapis.com
gcloud services enable eventarc.googleapis.com
gcloud services enable pubsub.googleapis.com
gcloud services enable storage.googleapis.com

gcloud projects add-iam-policy-binding dvtapp  --member="serviceAccount:service-809726561816-gs-projec@dvtapp.iam.gserviceaccount.com" --role="roles/storage.objectViewer"
gcloud projects add-iam-policy-binding dvtapp  --member="serviceAccount:service-809726561816-gs-projec@dvtapp.iam.gserviceaccount.com" --role="roles/storage.objectCreator"
gcloud projects add-iam-policy-binding dvtapp  --member="serviceAccount:service-809726561816-gs-projec@dvtapp.iam.gserviceaccount.com"  --role="roles/pubsub.publisher"

gsutil iam get gs://dvtapp_bucket_receiving_json_files

gsutil iam ch serviceAccount:service-809726561816-gs-projec@dvtapp.iam.gserviceaccount.com:roles/storage.objectViewer gs://dvtapp_bucket_receiving_json_files
gsutil iam ch serviceAccount:service-809726561816-gs-projec@dvtapp.iam.gserviceaccount.com:roles/storage.objectCreator gs://dvtapp_bucket_receiving_json_files

gcloud projects add-iam-policy-binding dvtapp --member="serviceAccount:service-809726561816-gs-projec@dvtapp.iam.gserviceaccount.com" --role="roles/eventarc.eventReceiver"