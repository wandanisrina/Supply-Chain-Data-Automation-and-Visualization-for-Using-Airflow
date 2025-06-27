'''
=================================================
Objective   : This program is desgined to automate the process of transforming and loading data from PostgreSQL to ElasticSearch. 
              Data used for analysis is supply chain pharmaceutical delivery dataset. Dataset will be transformed to clean data using Airflow DAG.
              This analysis aims to provide key metrics such as delivery timelines, shipment quantities, freight costs, and vendor performance for SVT Distributor Company.
              Furthermore, cleaned data will be visualized using Kibana to support better decision-making in supply chain management.

=================================================
'''

import pandas as pd
import numpy as np
from datetime import datetime
import re
from pandas.tseries.offsets import DateOffset
from sqlalchemy import create_engine
from airflow import DAG
from airflow.decorators import task
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from elasticsearch import Elasticsearch
import time

default_args= {
    'owner': 'Wanda',
    'start_date': datetime(2024, 11, 1)
}

def readCSV():
    '''
    Function used to read CSV file
    '''
    df = pd.read_csv('/opt/airflow/data/data_clean.csv')
    print(df.head())

with DAG(
    dag_id='DAG_Project_1',
    description='ETL process: extract from PostgreSQL, transform data, load to Elasticsearch',
    schedule_interval='10,20,30 9 * * 6',
    default_args=default_args,
    catchup=False
) as dag:
        start = EmptyOperator(task_id='start')
        end = EmptyOperator(task_id='end')

        @task()
        def extract_to_db():
            '''
            Function used to extract data from PostgreSQL database to CSV file
            
            - Connects to PostgreSQL using psycopg2.
            - Reads all records from 'table_project1'.
            - Saves the result to a raw CSV file.
    
            '''
            database = "airflow" #PostgreSQL database name
            username = "airflow" #PostgreSQL username
            password = "airflow" #PostgreSQL password
            host = "postgres" #PostgreSQL host (container name)

            postgres_url = f"postgresql+psycopg2://{username}:{password}@{host}/{database}" #PostgreSQL location url

            engine = create_engine(postgres_url)
            conn = engine.connect()
            df = pd.read_sql('select * from table_project1',conn) #Read data from PostgreSQL table
            df.to_csv('/opt/airflow/data/data_raw.csv') #output file path in csv format
            print("Data successfully extracted from PostgreSQL")

            return '/opt/airflow/data/data_raw.csv'

        @task()
        def transform():
            '''
            Function used to transform data by cleaning, normalizing, and imputing missing values.

            - Normalize column names.
            - Handle missing values.
            - Convert and clean date columns.
            - Convert categorical values to boolean/numeric where needed.
            - Save cleaned dataset as a new CSV.
            '''
            df = pd.read_csv('/opt/airflow/data/data_raw.csv')

            # Remove Duplicates (no duplicates)
            df.drop_duplicates(inplace=True)

            #Data Normalization
            def normalize_column(col):
                col = col.strip().lower()  # Trim and lowercase
                col = re.sub(r'[^\w]+', '_', col)  # change symbols (non alphabet&numeric) to _
                col = re.sub(r'_+', '_', col)      # Change multiple underscores to single
                col = col.strip('_')               # Delete underscore in the beginning and end
                return col
            df.columns = [normalize_column(col) for col in df.columns]

            #Handle Missing Values
            #Handling missing value in shipment mode column using mode value based on country
            df['shipment_mode'] = df.groupby('country')['shipment_mode'].transform(lambda x: x.fillna(x.mode()[0]))

            #Handling missing value in date columns
            date_cols = ['pq_first_sent_to_client_date', 'po_sent_to_vendor_date','scheduled_delivery_date', 'delivered_to_client_date', 'delivery_recorded_date']
            for col in date_cols:
                # Change string values (not datetime) to NaT
                df[col] = df[col].replace(['Date Not Captured', 'Pre-PQ Process', 'N/A - From RDC'], pd.NaT)

                # Konversi ke datetime
                df[col] = pd.to_datetime(df[col], errors='coerce', infer_datetime_format=True)

            #Handle missing value in po_sent_to_vendor_date colums
            handle_missing = df['po_sent_to_vendor_date'].isna()
            df.loc[handle_missing, 'po_sent_to_vendor_date'] = df.loc[handle_missing, 'scheduled_delivery_date'] - DateOffset(months=6) #replace by 6 month from scheduled_delivery_date
            #Handle missing value in pq_first_sent_to_client_date colums
            handle_missing_1 = df['pq_first_sent_to_client_date'].isna()
            df.loc[handle_missing_1, 'pq_first_sent_to_client_date'] = df.loc[handle_missing_1, 'po_sent_to_vendor_date'] - DateOffset(months=6) #replace by 6 month from po_sent_to_vendor_date

            #Handle missing value in dosage column
            df['dosage'] = df['dosage'].replace('N/A', np.nan)
            df['dosage'] = df['dosage'].fillna('no_dosage')

            #Handle missing value in weight column
            df['weight_kilograms'] = pd.to_numeric(df['weight_kilograms'], errors='coerce')
            median_weights = df.groupby(['line_item_quantity', 'product_group'])['weight_kilograms'].median()
            def impute_weight(row):
                if pd.isna(row['weight_kilograms']):
                    return median_weights.get((row['line_item_quantity'], row['product_group']), np.nan)
                else:
                    return row['weight_kilograms']
            df['weight_kilograms'] = df.apply(impute_weight, axis=1)
            # Handle another missing value in weight_kilograms column
            median_per_product = df.groupby('product_group')['weight_kilograms'].transform('median')
            df['weight_kilograms'] = df['weight_kilograms'].fillna(median_per_product)

            #Handling missing value in freight_cost_usd column
            df['freight_cost_usd'] = df['freight_cost_usd'].replace('Freight Included in Commodity Cost' , '0')
            df['freight_cost_usd'] = pd.to_numeric(df['freight_cost_usd'], errors='coerce')
            median_freight = df.groupby(['country', 'shipment_mode'])['freight_cost_usd'].median()
            def impute_freight(row):
                if pd.isna(row['freight_cost_usd']):
                    return median_freight.get((row['country'], row['shipment_mode']), np.nan)
                else:
                    return row['freight_cost_usd']
            df['freight_cost_usd'] = df.apply(impute_freight, axis=1)
            df['freight_cost_usd'] = df.apply(impute_freight, axis=1)
            # Handle another missing value in freight_cost_usd column
            median_per_shipment = df.groupby('shipment_mode')['freight_cost_usd'].transform('median')
            df['freight_cost_usd'] = df['freight_cost_usd'].fillna(median_per_shipment)

            #Handle missing value in line_item_insurance_usd column
            df['line_item_insurance_usd'].fillna(0, inplace=True)

            #Change column first_line_designation to Boolean
            df['first_line_designation'] = df['first_line_designation'].astype(str).str.strip().str.lower().map({'yes': True, 'no': False})

            print('Data transformation completed')
            print(df.head())
            clean_path = '/opt/airflow/data/data_clean.csv'
            df.to_csv(clean_path, index=False)

            return clean_path

        @task
        def loading():
            '''
            Function to load data to ElasticSearch
            - Reads cleaned CSV
            - Sends document to Elasticsearch index named 'data'.
            '''
            es = Elasticsearch('http://elasticsearch:9200')

            # Read data file
            df = pd.read_csv('/opt/airflow/data/data_clean.csv')

            # Send data to ElasticSearch
            for i, row in df.iterrows():
                es.index(index='data', id=i+1, body=row.to_json())
            

        
        start >> extract_to_db() >> transform() >> loading() >> end