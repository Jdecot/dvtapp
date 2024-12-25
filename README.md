# dvtapp
Data Engineering pipeline with GCP. A file arrives in Cloud Storage then a Cloud Function is triggered and the file's data are loaded into BigQuery table. 



## Things done
Create pipeline.py
Create service account and download it
Manage IAM rights for service account

Install poetry
Install poetry-plugin-dotenv
Create .env file at root and add env variable : path to json SA
All poetry commands then incorporate these env variables

Install Docker Desktop
Connecter Docker à Artifact Registry : gcloud auth configure-docker northamerica-northeast1-docker.pkg.dev


## Dev history
pipeline_old : first try
pipeline_working_1 : first working on gcp with credentials loaded from json file
pipeline_working_2 : first working on local with writing of json data file to another file
pipeline_working_3 : Pipeline now extract schema from json data and write it in another file
pipeline_working_6 : Pipeline detects well the type of json data and create SchemaField according to them, can add column with right schemafield
pipeline_working_7 : Data insertion works and retry mechanism is on


## Service account
C:\jsongcp\dvtapp-198e02b1453a.json

## Autre
export GOOGLE_APPLICATION_CREDENTIALS=C:\jsongcp\dvtapp-198e02b1453a.json
desktop-ukccead\julie

Connaître son domaine et username windows : desktop-ukccead\julie
Pour ajouter un utilisateur à partir du cmd : net localgroup docker-users desktop-ukccead\julie /add



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

## Enchainement des commandes

### 1 - Tester en local
poetry run python dvtapp/main.py --runner=DirectRunner --project=dvtapp 

avec flask et l'argument nom du fichier :
poetry run python dvtapp/main.py --runner=DirectRunner --project=dvtapp --input gs://dvtapp_bucket_receiving_json_files/data.json
curl http://127.0.0.1:8080?filename=mon_fichier.txt
curl -X POST -H "Content-Type: application/json" -d "{\"filename\": \"gs://dvtapp_bucket_receiving_json_files/data.json\"}" http://127.0.0.1:8080




### 2 - Builder image
docker build -t dvtapp-image-9 .

### 3 - Tester l'image

Voir le contenu de l'image :
docker run -it dvtapp-image-9 bash

Version sans Flask, Avec argument nom du fichier de data : 
    docker run -v C:\jsongcp:/app/config -e GOOGLE_APPLICATION_CREDENTIALS=/app/config/dvtapp-198e02b1453a.json dvtapp-image-9 python dvtapp/main.py gs://dvtapp_bucket_receiving_json_files/data.json

Version flask : 
    docker run -d -p 8080:8080 -v C:\jsongcp:/app/config -e GOOGLE_APPLICATION_CREDENTIALS=/app/config/dvtapp-198e02b1453a.json dvtapp-image-9 python dvtapp/main.py
    curl -X POST -H "Content-Type: application/json" -d "{\"filename\": \"gs://dvtapp_bucket_receiving_json_files/data.json\"}" http://localhost:8080

Pour afficher les logs en temps réel
    docker exec -it <container_id> tail -f /app/app.log 
    docker logs -f <container_id>

Arrêter et supprimer les serveurs 
    lister les conteneurs : docker ps
    docker stop $(docker ps -a -q)
    docker rm $(docker ps -a -q)

### 4 - Envoyer l'image dans GCR
docker tag dvtapp-image-9 northamerica-northeast1-docker.pkg.dev/dvtapp/dvtappdepot/dvtapp-image-9
docker push northamerica-northeast1-docker.pkg.dev/dvtapp/dvtappdepot/dvtapp-image-9

### 5 - Checker si l'image est dans GCR
gcloud container images list-tags northamerica-northeast1-docker.pkg.dev/dvtapp/dvtappdepot/dvtapp-image-9


### 6 - Déployer l'image avec Cloud Run
<!-- gcloud run deploy dvtapp-service --image=gcr.io/dvtapp/dvtapp-image-9 --region=us-east1 -->
gcloud run deploy dvtapp-service --image=northamerica-northeast1-docker.pkg.dev/dvtapp/dvtappdepot/dvtapp-image-9 --region=us-east1

<!-- En version Cloud Run Job : 
gcloud run jobs create dvtapp-job --image=northamerica-northeast1-docker.pkg.dev/dvtapp/dvtappdepot/dvtapp-image-9 --region=us-east1
gcloud run jobs execute dvtapp-job --region=us-east1 -->

Par défaut, Cloud Run expose un endpoint HTTP à l'URL : https://<service-name>.run.app
Tester l'endpoint : 
curl -X POST https://votre-service.run.app -d '{"file": "gs://votre-bucket/votre-fichier"}'

Service url : https://dvtapp-service-809726561816.us-east1.run.app

### 7 - Déployer la cloud function
<!-- gcloud functions deploy hello_gcs --runtime python39 --trigger-resource gs://my-bucket --trigger-event google.storage.object.finalize

gcloud functions deploy hello_gcs --runtime python310 --trigger-resource gs://dvtapp_bucket_receiving_json_files --trigger-event google.storage.object.finalize --source dvtapp\cloud_function.py
gcloud functions deploy hello_gcs --runtime python310 --trigger-resource gs://dvtapp_bucket_receiving_json_files --trigger-event google.storage.object.finalize --source dvtapp
gcloud functions deploy hello_gcs --runtime python310 --trigger-resource gs://dvtapp_bucket_receiving_json_files --trigger-event google.storage.object.finalize --source "C:/Users/Julie/OneDrive/Documents/devoteamapp/app/dvtapp/cf/"
gcloud functions deploy hello_gcs --runtime python310 --trigger-resource gs://dvtapp_bucket_receiving_json_files --trigger-event google.storage.object.finalize --source "C:/cf/"

gcloud functions deploy hello_gcs --runtime python310 --trigger-resource gs://dvtapp_bucket_receiving_json_files --trigger-event google.storage.object.finalize --location=us-east1 --source "C:/Users/Julie/OneDrive/Documents/devoteamapp/app/dvtapp/cf/" -->
<!-- gcloud functions deploy hello_gcs --runtime python310 --trigger-resource gs://dvtapp_bucket_receiving_json_files --trigger-event google.storage.object.finalize --region=us-east1 --source "C:/Users/Julie/OneDrive/Documents/devoteamapp/app/dvtapp/cf/" 

gcloud functions deploy hello-gcs  --trigger-event google.storage.object.finalize  --trigger-resource gs://dvtapp_bucket_receiving_json_files --runtime python39  --region us-east1 -->

gcloud functions deploy hello_gcs --runtime python310 --trigger-resource dvtapp_bucket_receiving_json_files --trigger-event google.storage.object.finalize --region us-east1 --source "C:/Users/Julie/OneDrive/Documents/devoteamapp/app/dvtapp/cf/"


## Comande Poetry
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


gcloud functions deploy hello_gcs --runtime python310 --trigger-resource dvtapp_bucket_receiving_json_files --trigger-event google.storage.object.finalize --region us-east1 --source "C:/Users/Julie/OneDrive/Documents/devoteamapp/app/dvtapp/cf/"




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