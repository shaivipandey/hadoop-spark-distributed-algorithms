import os
import requests
import json

# Configuration
SPARK_MASTER_URL = "http://172.20.16.168:4040"  # Update with your Spark master URL
# OUTPUT_DIR = "/Users/kritikkumar/Documents/DIC_summer_2024/shaivipa_kritikku_assignment_3/src/dijkstras/analysis_data/spark_ui_data_part2"
OUTPUT_DIR = "/Users/kritikkumar/Documents/DIC_summer_2024/shaivipa_kritikku_assignment_3/src/pagerank/analysis_data/spark_ui_data_part3"
# Create output directory if it doesn't exist
os.makedirs(OUTPUT_DIR, exist_ok=True)

def fetch_and_save(url, filename):
    response = requests.get(url)
    if response.status_code == 200:
        with open(os.path.join(OUTPUT_DIR, filename), 'w') as file:
            json.dump(response.json(), file, indent=4)
    else:
        print(f"Failed to fetch {url}, status code: {response.status_code}")

def fetch_dag_images(app_id, base_url):
    stages_url = f"{base_url}/api/v1/applications/{app_id}/stages"
    stages = requests.get(stages_url).json()

    for stage in stages:
        stage_id = stage['stageId']
        dag_url = f"{base_url}/stages/stage/?id={stage_id}&attempt=0"
        response = requests.get(dag_url)
        if response.status_code == 200:
            dag_image_path = os.path.join(OUTPUT_DIR, f"stage_{stage_id}_dag.png")
            with open(dag_image_path, 'wb') as file:
                file.write(response.content)
        else:
            print(f"Failed to fetch DAG for stage {stage_id}, status code: {response.status_code}")

def fetch_spark_ui_data():
    spark_ui_url = SPARK_MASTER_URL
    app_url = f"{spark_ui_url}/api/v1/applications"
    applications = requests.get(app_url).json()

    for app in applications:
        app_id = app['id']
        print(f"Fetching data for application {app_id}")

        # Create directory for this application
        app_output_dir = os.path.join(OUTPUT_DIR, app_id)
        os.makedirs(app_output_dir, exist_ok=True)

        app_base_url = f"{spark_ui_url}/api/v1/applications/{app_id}"

        # Fetch and save application details
        fetch_and_save(app_base_url, f"{app_output_dir}/{app_id}_details.json")

        # Fetch and save job details
        fetch_and_save(f"{app_base_url}/jobs", f"{app_output_dir}/{app_id}_jobs.json")

        # Fetch and save stage details
        fetch_and_save(f"{app_base_url}/stages", f"{app_output_dir}/{app_id}_stages.json")

        # Fetch and save executor details
        fetch_and_save(f"{app_base_url}/executors", f"{app_output_dir}/{app_id}_executors.json")

        # Fetch and save environment details
        fetch_and_save(f"{app_base_url}/environment", f"{app_output_dir}/{app_id}_environment.json")

        # Fetch and save storage details
        fetch_and_save(f"{app_base_url}/storage/rdd", f"{app_output_dir}/{app_id}_storage_rdd.json")

        # Fetch and save SQL details if applicable
        fetch_and_save(f"{app_base_url}/sql", f"{app_output_dir}/{app_id}_sql.json")

        # Fetch DAG images for stages
        fetch_dag_images(app_id, spark_ui_url)

if __name__ == "__main__":
    fetch_spark_ui_data()
    print("Data collection completed.")





