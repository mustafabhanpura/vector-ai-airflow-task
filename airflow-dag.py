import psycopg2
import requests
import json
import pandas as pd
from datetime import timedelta,datetime
from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
import os
import numpy as np
import psycopg2.extras as extras

"""
Following code is a simple sequential pipeline orchestrated through Airflow.

The entire pipeline is divided into 4 components:

1) fetch_json: This component fetches the latest data from the provided api(in this case latest=2019) 
               and stores into a json file. Before storing the content of the request in the json file
               unnecessary key-value content is removed from the response content.
2)get_csv: This component converts the downloaded json file into its equivalent csv file.
3)create_table: This component simply creates an empty table in the Postgres Database.
4)insert_values: This component inserts the data stored in the csv file into the Postgres table created 
                 in the previous component.

For Postgres connection created a custom connection in the Airflow UI and used the Postgres Hooks to create the databse connection.

"""


def fetch(url:str,ti):
    response = requests.get(url)
    json_content = json.loads(response.text)
    # print(json_content.keys())
    
    #The folders needed to be created before hand
    file_location = str(os.getcwd()+'\data\json\data.json')
    
    with open(file_location,"w") as outfile:
        #Only dumping the json content of key='data'
        json.dump(json_content['data'],outfile)
    #pushing the file location into ti to be read by the subsquent component
    ti.xcom_push(key='file_location',value=file_location)


def get_csv(ti):
    
    file_location = ti.xcom_pull(key='file_location',tasks_id=['fetch'])
    df = pd.read_json(file_location)
    csv_path = str(os.getcwd()) + '\data\csv\data.csv'
    df.to_csv(csv_path)
    ti.xcom_push(key='csv_file_location',value='csv_path')


def postgres_setup():
    create_table_query = """
        CREATE IF NOT EXISTS TABLE US_Population (
        ID_STATE VARCHAR(255) PRIMARY KEY,
        STATE VARCHAR(255) NOT NULL,
        YEAR INTEGER,
        POPULATION INTEGER
    )
    """
    #Using the inbuilt Postgres Providers to connect with the localhost
    #created a postgres connection within my local airflow environment
    hook = PostgresHook(postgre_conn_id='postgres',schema='postgres')
    conn = None

    try:
        conn = hook.get_conn() 
        cur = conn.cursor()
        cur.execute(create_table_query)
        cur.close()
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()


def insert_values(ti):
      
    conn = PostgresHook(postgre_conn_id='postgres',schema='postgres')
    table ='US_Population'
    file_location = ti.xcom_pull(key='csv_file_location',tasks_id=['get_csv'])
    
    data= pd.read_csv(file_location,header=0)
    
    df = data[['ID State','State','Year','Population']]

    #renaming the column name to match the schema of the created table
    df.rename(columns = {'ID State':'ID_STATE'}, inplace = True)

    tuples = [tuple(x) for x in df.to_numpy()]
  
    cols = ','.join(list(df.columns))
    # SQL query to execute
    query = "INSERT INTO %s(%s) VALUES %%s" % (table, cols)
    print(query)
    cursor = conn.cursor()
    try:
        extras.execute_values(cursor, query, tuples)
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
        conn.rollback()
        cursor.close()
        return 1
    print("the dataframe is inserted")
    cursor.close()

#DAG Definition

default_args = {
    'owner': 'mustafa',
    'depends_on_past': True,
    'start_date': days_ago(2),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}
with DAG(
  'vector-pipeline',
  tags=['extraction'],
  default_args = default_args
  ) as dag:

  fetch_json = PythonOperator(
      task_id = 'fetch',
      python_callable = fetch,
      op_kwargs = {'url':'https://datausa.io/api/data?drilldowns=State&measures=Population&year=latest'},
      dag = dag
  )

  get_csv_file= PythonOperator(
      task_id = 'get_csv',
      python_callable = get_csv,
      dag = dag
  )
    
  create_table = PythonOperator(
      task_id = 'postgres_setup',
      python_callable = postgres_setup,
      dag = dag
  )

  insert_values_into_tables = PythonOperator(
      task_id ='insert_values',
      python_callable=insert_values,
      dag = dag
  ) 

  fetch_json >> get_csv_file >> create_table >> insert_values_into_tables  
  
