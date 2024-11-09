from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

from datetime import datetime, timedelta
from load_csv_data import load_authors, load_keywords, load_labs, load_doi_authors, load_doi_keywords
from load_json_data import load_json_data
from data_cleaning import (
    clean_authors_data, 
    clean_keywords_data, 
    clean_labs_data, 
    clean_doi_authors_data, 
    clean_doi_keywords_data,
    clean_json_data as clean_json_func
)
from create_tables import insert_csv_data_DB , insert_json_data_DB

# Fonctions de nettoyage pour chaque fichier CSV
def clean_authors_task(**kwargs):
    authors_df = kwargs['ti'].xcom_pull(task_ids='load_authors')
    return clean_authors_data(authors_df)

def clean_keywords_task(**kwargs):
    keywords_df = kwargs['ti'].xcom_pull(task_ids='load_keywords')
    return clean_keywords_data(keywords_df)

def clean_labs_task(**kwargs):
    labs_df = kwargs['ti'].xcom_pull(task_ids='load_labs')
    return clean_labs_data(labs_df)

def clean_doi_authors_task(**kwargs):
    doi_authors_df = kwargs['ti'].xcom_pull(task_ids='load_doi_authors')
    return clean_doi_authors_data(doi_authors_df)

def clean_doi_keywords_task(**kwargs):
    doi_keywords_df = kwargs['ti'].xcom_pull(task_ids='load_doi_keywords')
    return clean_doi_keywords_data(doi_keywords_df)

# Fonction de nettoyage des données JSON
def clean_json_task(**kwargs):
    json_data = kwargs['ti'].xcom_pull(task_ids='load_json_data')
    return clean_json_func(json_data)

# Fonction pour insérer les données CSV combinées dans PostgreSQL
def insert_csv_data_task(**kwargs):
    cleaned_data = {
        "authors": kwargs['ti'].xcom_pull(task_ids='clean_authors_data'),
        "keywords": kwargs['ti'].xcom_pull(task_ids='clean_keywords_data'),
        "labs": kwargs['ti'].xcom_pull(task_ids='clean_labs_data'),
        "doi_authors": kwargs['ti'].xcom_pull(task_ids='clean_doi_authors_data'),
        "doi_keywords": kwargs['ti'].xcom_pull(task_ids='clean_doi_keywords_data')
    }
    insert_csv_data_DB(cleaned_data)

# Fonction pour insérer les données JSON dans PostgreSQL
def insert_json_data_task(**kwargs):
    # Pull cleaned data from XCom
    cleaned_json_data = kwargs['ti'].xcom_pull(task_ids='clean_json_data')
    
    # Call insert function with cleaned data as an argument
    insert_json_data_DB(cleaned_json_data)
# DAG pour orchestrer les étapes
with DAG(
    'data_etl',
    default_args={'owner': 'airflow'},
    schedule_interval=timedelta(weeks=1),
    #schedule_interval=timedelta(minutes=2),
    start_date=datetime(2024, 11, 9),
    catchup=False
) as dag:

    # Tâches de chargement de données JSON
    load_json_data_task = PythonOperator(
        task_id='load_json_data',
        python_callable=load_json_data,
    )

    # Tâches de chargement de données CSV
    load_authors_task = PythonOperator(
        task_id='load_authors',
        python_callable=load_authors,
    )
    
    load_keywords_task = PythonOperator(
        task_id='load_keywords',
        python_callable=load_keywords,
    )
    
    load_labs_task = PythonOperator(
        task_id='load_labs',
        python_callable=load_labs,
    )
    
    load_doi_authors_task = PythonOperator(
        task_id='load_doi_authors',
        python_callable=load_doi_authors,
    )
    
    load_doi_keywords_task = PythonOperator(
        task_id='load_doi_keywords',
        python_callable=load_doi_keywords,
    )

    # Tâches de nettoyage des données
    
    # json
    clean_json_data_task = PythonOperator(
        task_id='clean_json_data',
        python_callable=clean_json_task,
        provide_context=True
    )
    
    # csv
    clean_authors_data_task = PythonOperator(
        task_id='clean_authors_data',
        python_callable=clean_authors_task,
        provide_context=True
    )
    
    clean_keywords_data_task = PythonOperator(
        task_id='clean_keywords_data',
        python_callable=clean_keywords_task,
        provide_context=True
    )
    
    clean_labs_data_task = PythonOperator(
        task_id='clean_labs_data',
        python_callable=clean_labs_task,
        provide_context=True
    )
    
    clean_doi_authors_data_task = PythonOperator(
        task_id='clean_doi_authors_data',
        python_callable=clean_doi_authors_task,
        provide_context=True
    )
    
    
    clean_doi_keywords_data_task = PythonOperator(
        task_id='clean_doi_keywords_data',
        python_callable=clean_doi_keywords_task,
        provide_context=True
    )



    # Tâches d'insertion dans PostgreSQL
    
    insert_json_table_task = PythonOperator(
        task_id='insert_json_table',
        python_callable=insert_json_data_task,
        provide_context=True
    )
    
    insert_csv_table_task = PythonOperator(
        task_id='insert_csv_table',
        python_callable=insert_csv_data_task,
        provide_context=True
    )
    
    run_visualization_script = BashOperator(
        task_id='run_visualization_script',
        bash_command='streamlit run visualisation.py'
    )
    
    # Orchestration des tâches
    load_json_data_task >> clean_json_data_task >> insert_json_table_task
    
    load_csv_data_task = [load_authors_task, load_keywords_task, load_labs_task, load_doi_authors_task, load_doi_keywords_task]
    clean_csv_data_task = [clean_authors_data_task, clean_keywords_data_task, clean_labs_data_task, clean_doi_authors_data_task, clean_doi_keywords_data_task]
    
    # Applique les dépendances pour chaque tâche
    for load_task, clean_task in zip(load_csv_data_task, clean_csv_data_task):
        load_task >> clean_task >> insert_csv_table_task

    insert_json_table_task >> insert_csv_table_task >> run_visualization_script

    
    #C insert_json_table_task >> insert_csv_table_task
