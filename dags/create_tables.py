import psycopg2
import os
from dotenv import load_dotenv
load_dotenv()

def get_postgres_connection():
    host = os.getenv("POSTGRES_HOST", "airflow-postgres-1")
    port = os.getenv("POSTGRES_PORT", 5432)
    database = os.getenv("POSTGRES_DB", "airflow")
    user = os.getenv("POSTGRES_USER", "airflow")
    password = os.getenv("POSTGRES_PASSWORD", "airflow")

    print(f"Connecting to PostgreSQL at {host}:{port}, database: {database}, user: {user}")

    conn = psycopg2.connect(
            host=host,
            port=port,
            database=database,
            user=user,
            password=password
        )
    return conn

# Function to insert cleaned CSV data into PostgreSQL
def insert_csv_data_DB(cleaned_csv_data):
    conn = get_postgres_connection()
    cursor = conn.cursor()

    try:
        for _, row in cleaned_csv_data['labs'].iterrows():
            cursor.execute("""
                INSERT INTO labs (LabID, LabName)
                VALUES (%s, %s) ON CONFLICT (LabID) DO NOTHING;
            """, (row['LabID'], row['Lab']))
            
        for _, row in cleaned_csv_data['authors'].iterrows():
            cursor.execute("""
                INSERT INTO authors (AuthorID, AuthorName, LabID)
                VALUES (%s, %s, %s) ON CONFLICT (AuthorID) DO NOTHING;
            """, (row['AuthorID'], row['AuthorName'], row['LabID']))

        for _, row in cleaned_csv_data['keywords'].iterrows():
            cursor.execute("""
                INSERT INTO keywords (KeywordID, Keyword, KeywordType)
                VALUES (%s, %s, %s) ON CONFLICT (KeywordID) DO NOTHING;
            """, (row['KeywordID'], row['Keyword'], row['KeywordType']))

        for _, row in cleaned_csv_data['doi_keywords'].iterrows():
            cursor.execute("""
                INSERT INTO doi_keywords (DOI, KeywordID)
                VALUES (%s, %s) ON CONFLICT (DOI, KeywordID) DO NOTHING;
            """, (row['DOI'], row['KeywordID']))

        for _, row in cleaned_csv_data['doi_authors'].iterrows():
            cursor.execute("""
                INSERT INTO author_labs (AuthorID, DOI)
                VALUES (%s, %s) ON CONFLICT (AuthorID, DOI) DO NOTHING;
            """, (row['AuthorID'], row['DOI']))

        conn.commit()
    except Exception as e:
        print(f"Error occurred: {e}")
        conn.rollback()
    finally:
        cursor.close()
        conn.close()
# Function to insert cleaned JSON data into PostgreSQL
def insert_json_data_DB(cleaned_json_data):
    conn = get_postgres_connection()
    cursor = conn.cursor()

    try:
        for _, row in cleaned_json_data.iterrows():
            cursor.execute("""
                INSERT INTO articles (DOI, Title, Link, Abstract, Date_of_Publication, Published_In)
                VALUES (%s, %s, %s, %s, %s, %s) ON CONFLICT (DOI) DO NOTHING;
            """, (row['DOI'], row['Title'], row['Link'], row['Abstract'], row['Date of Publication'], row['Published In']))

        conn.commit()

    except Exception as e:
        print(f"Error occurred: {e}")
        conn.rollback()
    
    finally:
        cursor.close()
        conn.close()
