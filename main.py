from google.cloud import bigquery
from google.oauth2 import service_account
import json
from tabulate import tabulate

# Load GCP credentials from the JSON file
service_account_info = json.load(open('C:/Users/vani/hello2/project_p1/gcp_json.json'))
credentials = service_account.Credentials.from_service_account_info(service_account_info)
project_id ='eloquent-drive-413114'
client = bigquery.Client(credentials=credentials, project=project_id)

# Set the dataset name
dataset_name = 'Divya_stocks11'

def select_data():
    query = f"SELECT * FROM `{project_id}.{dataset_name}.stocks`"
    try:
        result = client.query(query).result()

        # Convert the result to a list of dictionaries
        result_list = [dict(row.items()) for row in result]

        if not result_list:
            print("No data")
            return []  # Return an empty list if there's no data
        else:
            headers = ["Company_ID", "Ticker", "Open", "High", "Low", "Close", "Adjclose", "Volume"]
            data_tuples = [(row['Company_ID'], row['Ticker'], row['Open'], row['High'], row['Low'], row['Close'], row['Adjclose'], row['Volume']) for row in result_list]
            return data_tuples
    except Exception as e:
        print(f"Error selecting data: {e}")
        return []  # Return an empty list in case of an error

def insert_data(Company_ID, Ticker, Open, High, Low, Close, Adjclose, Volume):
    query = f"""
        INSERT INTO `{project_id}.{dataset_name}.stocks`(Company_ID, Ticker, Open, High, Low, Close, Adjclose, Volume)
        VALUES ('{Company_ID}', '{Ticker}', {Open}, {High}, {Low}, {Close}, {Adjclose}, {Volume})
    """
    client.query(query)

def update_data(Company_ID, New_Company):
    query = f"""
        UPDATE `{project_id}.{dataset_name}.stocks`
        SET Ticker = @new_company
        WHERE Company_ID = @company_id
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("new_company", "STRING", New_Company),
            bigquery.ScalarQueryParameter("company_id", "STRING", Company_ID),
        ]
    )
    
    try:
        query_job = client.query(query, job_config=job_config)
        print(f"Query executed successfully: {query_job.query}")
        print(f"Rows affected: {query_job.num_dml_affected_rows}")
        print("Data updated successfully.")
    except Exception as e:
        print(f"Error updating data: {e}")

def delete_data(Company_ID):
    query = f"""
        DELETE FROM `{project_id}.{dataset_name}.stocks`
        WHERE Company_ID = '{Company_ID}'
    """
    client.query(query)

def main():
    role = input("Enter your role (1 for Administrator, 2 for User): ")
    while role not in ['1', '2']:
        print("Invalid role. Please enter 1 for Administrator or 2 for User.")
        role = input("Enter your role (1 for Administrator, 2 for User): ")
    
    while True:
        print("\nOperations:")
        print("1. Insert Data")
        print("2. Update Data")
        print("3. Delete Data")
        print("4. Display Data")
        print("5. Exit")
        operation = input("Enter the operation number you want to perform: ")

        if operation == '1' and role == '1':  # Insert Data
            Company_ID = input("Enter the Company_ID: ")
            Ticker = input("Enter name: ")
            Open = float(input("Enter the Open value: "))
            High = float(input("Enter the High value: "))
            Low = float(input("Enter the Low value: "))
            Close = float(input("Enter the Close value: "))
            Adjclose = float(input("Enter the Adjclose value: "))
            Volume = int(input("Enter the Volume value: "))

            insert_data(Company_ID, Ticker, Open, High, Low, Close, Adjclose, Volume)
            print("Data inserted successfully.")

        elif operation == '2' and role == '1':  # Update Data
            Company_ID = input("Enter Company_ID to update: ")
            New_Company = input("Enter the New_Company: ")
            update_data(Company_ID, New_Company)
      
        elif operation == '3' and role == '1':  # Delete Data
            Company_ID = input("Enter Company_ID to delete: ")
            delete_data(Company_ID)
            print("Data deleted successfully.")

        elif operation == '4':  # Display Data
            data = select_data()
            print("Table:")
            print(tabulate(data, headers=["Company_ID", "Ticker", "Open", "High", "Low", "Close", "Adjclose", "Volume"]))

        elif operation == '5':  # Exit
            break

        else:
            print("Invalid operation. Please try again.")

if __name__ == "__main__":
    main()
