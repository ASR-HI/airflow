import pandas as pd


def load_authors():
    return pd.read_csv('/opt/airflow/dags/data/authors.csv')

def load_keywords():
    return pd.read_csv('/opt/airflow/dags/data/keywords.csv')

def load_labs():
    return pd.read_csv('/opt/airflow/dags/data/labs.csv')

def load_doi_authors():
    return pd.read_csv('/opt/airflow/dags/data/doi_authors.csv')

def load_doi_keywords():
    return pd.read_csv('/opt/airflow/dags/data/doi_keywords.csv')


def load_csv_data():
    author_df = pd.read_csv('/opt/airflow/dags/data/authors.csv')    
    keywords_df = pd.read_csv('/opt/airflow/dags/data/keywords.csv')
    labs_df = pd.read_csv('/opt/airflow/dags/data/labs.csv')
        
    doi_authors_df = pd.read_csv('/opt/airflow/dags/data/doi_authors.csv')
    doi_keywords_df = pd.read_csv('/opt/airflow/dags/data/doi_keywords.csv')


    return {
        "authors": author_df,
        "keywords": keywords_df,
        "labs": labs_df,
        "doi_keywords": doi_keywords_df,
        "doi_authors": doi_authors_df,
    }
