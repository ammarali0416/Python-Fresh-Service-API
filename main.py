# -*- coding: utf-8 -*-
"""
Created on Fri Aug 25 00:58:35 2023

@author: ammar.ali

Script to fetch data from Freshservice and load it into an Azure Blob storage directory.
"""
import logging
import pandas as pd
import requests
import re
from snowflake.snowpark import Session
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
from io import StringIO

api_creds_path = r'C:\Credentials\FS_API_KEY.csv' # directory holding the API key
sf_creds_path = r'C:\Credentials\SNOWPARK_DEV_LOADER_CREDS.CSV' # directory holding the Snowpark credentials

account_name = '' # your company's Azure Storage account name
container_name = '' # azure blob storage container name
sas_token = '' # your azure blob storage shared access signature used for authentication

domain = '' # your company's FreshService Domain  
password = 'X'  # 'x' is sufficient since API key is used for authentication

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s') # initialize the logging

def create_session(creds_path):
    """
    Creates a Snowflake session using the provided credentials path.
    This function assumes you are configuring your credentials file as the connection parameters dictionary done here: 
    https://docs.snowflake.com/en/developer-guide/snowpark/python/creating-session
    Parameters:
    - creds_path: A string specifying the directory of you Snowflake credentials csv
    Returns:
    - A Snowflake session object
    """
    df = pd.read_csv(creds_path, header=None)
    connection_parameters = dict(zip(df[0], df[1]))
    logging.info("Creating Snowflake session...")
    return Session.builder.configs(connection_parameters).create()

def get_api_key(creds_path):
    """
    Get the FS API key from a credentials file
    Parameters:
    - creds_path: A string specifying the directory of you FreshService credentials csv
    Returns:
    - A string containing your FreshService API
    """
    df = pd.read_csv(creds_path, header=None)
    return df[0][0]

def get_updated_timestamp(session):
    """
    Fetches the maximum timestamp value (either UPDATED_AT or CREATED_AT) from the TICKETS table in Snowflake.
    If no timestamp is found, it defaults to '2001-04-16T00:00:00Z'.
    
    Parameters:
    - session: A Snowflake session object used to execute SQL queries.
    
    Returns:
    - str: A string representation of the timestamp in ISO 8601 format.
    """
    
    # Execute SQL query to get the maximum timestamp from TICKETS table
    query_result = session.sql(
        "SELECT IFNULL(MAX(COALESCE(UPDATED_AT, CREATED_AT)), '2001-04-16T00:00:00Z') AS X FROM TICKETS"
    )
    
    # Convert the SQL result to a Pandas DataFrame
    df = query_result.toPandas()
    
    # Extract the timestamp value from the DataFrame
    timestamp_value = df['X'][0]
    
    # Convert the timestamp to a string
    timestamp_str = str(timestamp_value)
    
    # Format the string to ISO 8601 format
    iso_formatted_timestamp = timestamp_str.replace(" ", "T") + "Z"
    
    return iso_formatted_timestamp

def fetch_data_from_endpoint(api_key, domain, password, endpoint, record_path):
    """
    Fetch data from the given Freshservice endpoint.

    Parameters:
    - api_key: A FreshService API key as a string
    - domain: Your company's FreshService API domain string
    - password: The password to access the FreshService API
    - endpoint: the end point to access from the API
    - record_path: the key to use when accessing the endpoint data from the json response
    Returns:
    - df_data: A data frame containing a normalized version of the json data 
    """
    
    headers = {'Content-Type': 'application/json'}
    
    url = f'https://{domain}.freshservice.com/api/v2/{endpoint}'
    logging.info(f"Fetching data from {url}...")
    
    all_data = []

    while url:
        try:
            response = requests.get(url, auth=(api_key, password), headers=headers)
            response.raise_for_status()  # Raise an HTTPError if the HTTP request returned an unsuccessful status code

            # Convert the JSON response to a DataFrame and add it to the list
            df = pd.json_normalize(response.json(), record_path=record_path)
            all_data.append(df)
            
            # Check for the 'Link' header for pagination
            link_header = response.headers.get('Link')
            match = re.search('<(.*?)>', link_header) if link_header else None
            url = match.group(1) if match else None

        except requests.RequestException as e:
            if response.status_code == 429:
                logging.warning("Rate limit reached. Waiting for 60 seconds before retrying...")
                time.sleep(60)
                continue
            else:
                logging.error(f"Error fetching data from {url}. Error: {e}")
                break

    # Concatenate all the data into one DataFrame
    df_data = pd.concat(all_data, ignore_index=True) if all_data else pd.DataFrame()
    
    # Convert column names to uppercase
    df_data.columns = df_data.columns.str.upper()

    logging.info(f"Finished fetching. Total records retrieved: {len(df_data)}.")
    
    return df_data

def upload_df_to_blob(df, account_name, container_name, sas_token, blob_path):
    """
    Uploads a DataFrame to Azure Blob Storage.
    
    Parameters:
    - df (DataFrame): The DataFrame to upload.
    - account_name (str): The name of the storage account.
    - container_name (str): The name of the blob container.
    - sas_token (str): The shared access signature token.
    - blob_path (str): The path where the blob should be saved.
    """
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False)
    
    blob_service_client = BlobServiceClient(account_url=f"https://{account_name}.blob.core.windows.net", credential=sas_token)
    blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_path)
    blob_client.upload_blob(csv_buffer.getvalue(), overwrite=True)


def main():
    try:
        session = create_session(sf_creds_path)
        api_key = get_api_key(api_creds_path)  
                
        # Find the latest data from your Snowflake TICKETS table
        updated_since = get_updated_timestamp(session)
        session.close() # close the session
        
        ticket_df = fetch_data_from_endpoint(api_key, domain, password, f'tickets?per_page=100&updated_since={updated_since}', 'tickets')    
        # Create a dictionary of columns to rename and rename the columns
        rename_cols = {col: col.replace("CUSTOM_FIELDS.", "") for col in ticket_df.columns if "CUSTOM_FIELDS." in col}
        ticket_df.rename(columns=rename_cols, inplace=True)
        
        # Fetch ticket fields
        ticket_fields_df = fetch_data_from_endpoint(api_key, domain, password, 'ticket_form_fields?per_page=100', 'ticket_fields')
        
        # Fetch agent groups data
        agent_groups_df = fetch_data_from_endpoint(api_key, domain, password, 'groups?per_page=100', 'groups')
                
        # Upload agent groups data to blob
        blob_path = 'API_FRESHSERVICE/AGENTGROUPS.csv'
        upload_df_to_blob(agent_groups_df, account_name, container_name, sas_token, blob_path)
    
        # Upload tickets data to blob
        blob_path = 'API_FRESHSERVICE/TICKETS.csv'
        upload_df_to_blob(ticket_df, account_name, container_name, sas_token, blob_path)
    
        # Upload ticket fields data to blob
        blob_path = 'API_FRESHSERVICE/TICKET_FIELDS.csv'
        upload_df_to_blob(ticket_fields_df, account_name, container_name, sas_token, blob_path)
        
    except Exception as e:
        if "SQL compilation error:" in str(e):  # Checking for specific Snowflake exception.
            logging.error("Encountered a SnowparkSQLException: %s", e)
        else:
            logging.error("Encountered an error: %s", e)
            raise
   
if __name__ == '__main__':
   main()
