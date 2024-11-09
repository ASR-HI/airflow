import pandas as pd
import os

def load_json_data():
    json_data = pd.read_json("/opt/airflow/dags/data/clean_ieee.json")
    return json_data
