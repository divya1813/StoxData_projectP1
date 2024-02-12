from google.cloud import bigquery
from google.oauth2 import service_account
import json
import os
from matplotlib.table import Cell
 
 
 
# Load your GCP credentials from the JSON file
credentials = service_account.Credentials.from_service_account_file('C:/Users/vani/hello2/project_p1/gcp_json.json')
project_id ='eloquent-drive-413114'
client = bigquery.Client(credentials= credentials,project=project_id)
 
 

# Set your dataset name
dataset_name = 'Divya_stocks11'    
 
 # Set the path to the directory containing CSV files
csv_files_dir =   'C:/Users/vani/hello2/project_p1/csv'
 
# # Create the dataset
dataset_id = f'{project_id}.{dataset_name}'
dataset = client.create_dataset(dataset_id, exists_ok=True)
 
# Get a list of CSV files in the directory
csv_files = [f for f in os.listdir(csv_files_dir) if f.endswith('.csv')]
 
# Load data into tables
for csv_file in csv_files:
   table_name = os.path.splitext(csv_file)[0]  # Remove the .csv extension to get the table name
   table_id = f'{dataset_id}.{table_name}'
   table_ref = client.dataset(dataset.dataset_id).table(table_name)
 
    # Load data from CSV file with auto-detected schema
   csv_file_path = os.path.join(csv_files_dir, csv_file)
   job_config = bigquery.LoadJobConfig(autodetect=True)
 
with open(csv_file_path, 'rb') as source_file:
    load_job = client.load_table_from_file(source_file, table_ref, job_config=job_config)
    load_job.result()  # Waits for the job to complete
 
print(f'Data loaded into {table_name} successfully with auto-detected schema.')
 
print('Process completed.')