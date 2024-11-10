# Data Pipeline Project with Apache Airflow


## Objectif du Projet

- Définir une pipeline permettant l’ingestion, la transformation et le chargement des données provenant de multiples sources (API, bases de données, fichiers, etc.) vers un entrepôt de données (Datawarehouse).
- Développer un tableau de bord permettant la visualisation des graphiques, indicateurs et rapports résultant de l’analyse des données.

## Architecture de la Pipeline

L'architecture de la pipeline de données est composée des étapes suivantes :
- **Ingestion des données** : Collecte des données depuis diverses sources (fichiers CSV, JSON, bases de données, API).
- **Nettoyage des données** : Transformation et nettoyage des données pour assurer leur qualité et leur cohérence.
- **Chargement des données** : Insertion des données nettoyées dans l'entrepôt de données.
- **Orchestration des tâches** : Utilisation d'Apache Airflow pour orchestrer et automatiser les différentes étapes de la pipeline.
- **Visualisation des données** : Développement d'un tableau de bord pour visualiser les données et faciliter la prise de décision.

## Technologies Utilisées

- **Apache Airflow** : Orchestration des workflows.
- **Streamlit** : Visualisation des données.
- **Docker** : Conteneurisation des applications.
- **PostgreSQL** : Stockage des données.
- **DBeaver** : Gestion des bases de données.

## Installation

### Prérequis

- Docker
- Python

### Étapes d'Installation

1. **Cloner le dépôt :**
    ```bash
    git clone https://github.com/ASR-HI/airflow
    cd airflow
    ```

2. **Lancer les services Docker :**
    ```bash
    docker-compose up 
    ```

3. **Installer les dépendances Python :**
    ```bash
    pip install -r requirements.txt
    ```




## Utilisation

1. **Accéder à l'interface web d'Airflow :**
    Ouvrez votre navigateur et allez à `http://localhost:8080`. Connectez-vous avec les identifiants suivants :
    - **Nom d'utilisateur** : `airflow`
    - **Mot de passe** : `airflow`


2. **Activer et exécuter le DAG :**
    Dans l'interface web d'Airflow, activez le DAG `data_etl` et exécutez-le manuellement ou attendez qu'il s'exécute selon le planning défini.

3. **Visualiser les données :**
    Ouvrez l'application Streamlit pour visualiser les données :
    ```bash
    streamlit run visualisation.py
    ```

## Structure du Projet

- `data_pipeline.py` : Définit le DAG et les tâches pour l'ingestion, le nettoyage et le chargement des données.
- `load_csv_data.py` : Contient les fonctions pour charger les données CSV.
- `load_json_data.py` : Contient les fonctions pour charger les données JSON.
- `data_cleaning.py` : Contient les fonctions pour nettoyer les données.
- `create_tables.py` : Contient les fonctions pour insérer les données dans PostgreSQL.
- `visualisation.py` : Contient le code pour l'application Streamlit de visualisation des données.


---

En suivant ce README, vous devriez être en mesure de configurer et d'exécuter votre pipeline de données avec Apache Airflow, ainsi que de visualiser les résultats dans une application Streamlit.