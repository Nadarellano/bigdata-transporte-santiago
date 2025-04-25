import requests
from google.cloud import pubsub_v1
from google.cloud import storage
import json
import os
import time

project_id = "thematic-mapper-423323-e8"
topic_id = "datos-transporte-publico-real-time"
bucket_name = "thematic-mapper-423323-e8-real-time"

def fetch_data_with_retries(url, retries=5, delay=10, timeout=60):
    for attempt in range(retries):
        try:
            response = requests.get(url, timeout=timeout)
            response.raise_for_status()
            return response.json()
        except (requests.exceptions.RequestException, ConnectionResetError) as e:
            print(f"Request error: {e}")
            if attempt < retries - 1:
                print(f"Retrying in {delay} seconds... (attempt {attempt + 1}/{retries})")
                time.sleep(delay)
            else:
                raise

codsint_list = [205, 210, 226, 101, 508]

publisher = pubsub_v1.PublisherClient()
storage_client = storage.Client()

for codsint in codsint_list:
    data_url = f"https://www.red.cl/restservice_v2/rest/conocerecorrido?codsint={codsint}"
    try:
        message = fetch_data_with_retries(data_url, timeout=120)  # Incrementar el tiempo de espera

        # Publica el mensaje en Pub/Sub
        topic_path = publisher.topic_path(project_id, topic_id)
        future = publisher.publish(topic_path, json.dumps(message).encode("utf-8"))
        future.result()  # Espera a que se complete la publicación
        print(f"Published message to {topic_path}")

        # Guarda el mensaje en un archivo local
        file_name = f"message_{codsint}.json"
        with open(file_name, "w") as f:
            json.dump(message, f)

        # Sube el archivo a Cloud Storage
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(file_name)
        blob.upload_from_filename(file_name)
        print(f"File {file_name} uploaded to {bucket_name}.")

        # Elimina el archivo local después de subirlo
        os.remove(file_name)
        print(f"File {file_name} removed from local storage.")
    except requests.exceptions.RequestException as e:
        print(f"Failed to fetch data for codsint {codsint} after multiple retries: {e}")
