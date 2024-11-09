import streamlit as st
import pandas as pd
import psycopg2
import os
from dotenv import load_dotenv
import matplotlib.pyplot as plt
import seaborn as sns

# Charger les variables d'environnement
load_dotenv()

# Fonction pour se connecter à la base de données PostgreSQL
def get_postgres_connection():
    host = "localhost"
    port = os.getenv("POSTGRES_PORT", 5432)
    database = os.getenv("POSTGRES_DB", "airflow")
    user = os.getenv("POSTGRES_USER", "airflow")
    password = os.getenv("POSTGRES_PASSWORD", "airflow")

    try:
        conn = psycopg2.connect(
            host=host,
            port=port,
            database=database,
            user=user,
            password=password
        )
        return conn
    except Exception as e:
        st.error(f"Error connecting to PostgreSQL: {e}")
        return None

# Fonction pour charger les données avec une requête SQL
def load_data(query):
    conn = get_postgres_connection()
    if conn:
        df = pd.read_sql_query(query, conn)
        conn.close()
        return df
    else:
        return pd.DataFrame()  # Return an empty DataFrame if the connection fails

# Configuration de l'interface Streamlit
st.title('Articles and Authors Visualization')

# Requête SQL pour obtenir les articles et les auteurs associés
articles_query = """
SELECT articles.*, author_article.AuthorID
FROM articles
JOIN author_article ON articles.DOI = author_article.DOI
"""

# Requêtes SQL pour obtenir les autres tables
authors_query = "SELECT * FROM authors"
labs_query = "SELECT * FROM labs"
keywords_query = "SELECT * FROM keywords"
author_article_query = "SELECT * FROM author_article"
doi_keywords_query = "SELECT * FROM doi_keywords"

# Charger les données des différentes tables
articles_df = load_data(articles_query)
authors_df = load_data(authors_query)
labs_df = load_data(labs_query)
keywords_df = load_data(keywords_query)
author_article_df = load_data(author_article_query)

# Afficher les données dans Streamlit
st.subheader('Articles')
st.dataframe(articles_df)

st.subheader('Authors')
st.dataframe(authors_df)

st.subheader('Labs')
st.dataframe(labs_df)

st.subheader('Keywords')
st.dataframe(keywords_df)

# Nombre de mots-clés
keywords_count = keywords_df['keyword'].value_counts()

# Nombre d'auteurs
authors_count = authors_df['authorname'].value_counts()

# Nombre de laboratoires
labs_count = labs_df['labname'].value_counts()

# Nombre d'articles
articles_count = articles_df['title'].value_counts()

# Plot pour les articles
plt.figure(figsize=(6, 4))
plt.bar(['Total Articles'], [articles_count.sum()], color='skyblue')
plt.ylabel('Nombre d\'articles')
plt.title('Total des articles')
st.pyplot(plt)

# Plot pour les auteurs
plt.figure(figsize=(6, 4))
plt.bar(['Total Authors'], [authors_count.sum()], color='lightgreen')
plt.ylabel('Nombre d\'auteurs')
plt.title('Total des auteurs')
st.pyplot(plt)

# Plot pour les mots-clés
plt.figure(figsize=(6, 4))
plt.bar(['Total Keywords'], [keywords_count.sum()], color='lightcoral')
plt.ylabel('Nombre de mots-clés')
plt.title('Total des mots-clés')
st.pyplot(plt)

# Plot pour les laboratoires
plt.figure(figsize=(6, 4))
plt.bar(['Total Labs'], [labs_count.sum()], color='lightskyblue')
plt.ylabel('Nombre de laboratoires')
plt.title('Total des laboratoires')
st.pyplot(plt)

# Joindre les tables pour compter le nombre d'auteurs par laboratoire
# Joindre 'authors', 'author_article' et 'labs' sur les bonnes clés
authors_labs_query = """
SELECT labs.labname, COUNT(DISTINCT authors.authorname) AS author_count
FROM labs
JOIN authors ON authors.labid = labs.labid
JOIN author_article ON author_article.AuthorID = authors.AuthorID
GROUP BY labs.labname
ORDER BY author_count DESC limit 20
"""

# Charger les données du nombre d'auteurs par laboratoire
authors_labs_df = load_data(authors_labs_query)

# Afficher le nombre d'auteurs par laboratoire dans un graphique
st.subheader('Nombre d\'auteurs par laboratoire (Top 20)')

# Créer un graphique à barres pour le nombre d'auteurs par laboratoire
plt.figure(figsize=(10, 6))
sns.barplot(x='author_count', y='labname', data=authors_labs_df.head(20), palette='viridis')
plt.xlabel('Nombre d\'auteurs')
plt.ylabel('Laboratoire')
plt.title('Nombre d\'auteurs dans chaque laboratoire')
st.pyplot(plt)



# Nombre d'articles par auteur
authors_articles_query = """
SELECT authors.authorname, COUNT(DISTINCT articles.DOI) AS article_count
FROM authors
JOIN author_article ON author_article.AuthorID = authors.AuthorID
JOIN articles ON articles.DOI = author_article.DOI
GROUP BY authors.authorname
ORDER BY article_count DESC
"""

# Charger les résultats pour le nombre d'articles par auteur
authors_articles_df = load_data(authors_articles_query)

# Afficher le nombre d'articles par auteur
st.subheader('Auteurs avec le plus grand nombre de publications (Top 20)')
plt.figure(figsize=(10, 6))
sns.barplot(x='article_count', y='authorname', data=authors_articles_df.head(20), palette='Blues_d')
plt.xlabel('Nombre d\'articles')
plt.ylabel('Auteur')
plt.title('Auteurs avec le plus grand nombre de publications')
st.pyplot(plt)

st.subheader('Auteurs avec le plus petit nombre de publications')
plt.figure(figsize=(10, 6))
sns.barplot(x='article_count', y='authorname', data=authors_articles_df.tail(20), palette='Blues_d')
plt.xlabel('Nombre d\'articles')
plt.ylabel('Auteur')
plt.title('Auteurs avec le plus petit nombre de publications')
st.pyplot(plt)

# Mots-clés les plus répétés
keywords_count_query = """
SELECT keywords.keyword, COUNT(doi_keywords.DOI) AS keyword_count
FROM keywords
JOIN doi_keywords ON doi_keywords.KeywordID = keywords.KeywordID
GROUP BY keywords.keyword
ORDER BY keyword_count DESC
"""

# Charger les résultats pour les mots-clés les plus répétés
keywords_count_df = load_data(keywords_count_query)

# Afficher les mots-clés les plus répétés
st.subheader('Mots-clés les plus répétés (top 20)')
plt.figure(figsize=(10, 6))
sns.barplot(x='keyword_count', y='keyword', data=keywords_count_df.head(20), palette='magma')
plt.xlabel('Nombre de publications')
plt.ylabel('Mot-clé')
plt.title('Mots-clés les plus répétés')
st.pyplot(plt)


st.subheader('Mots-clés les moins répétés')
plt.figure(figsize=(10, 6))
sns.barplot(x='keyword_count', y='keyword', data=keywords_count_df.tail(20), palette='magma')
plt.xlabel('Nombre de publications')
plt.ylabel('Mot-clé')
plt.title('Mots-clés les moins répétés')
st.pyplot(plt)








# Liste des mots-clés pour le filtrage
keywords = ['llm', 'devops', 'ai', 'blockchain', 'cloud', 'big data', 'iot', 'cybersecurity', 'quantum', '5g','Multimodal llm']

# Filtrer les articles dont le titre contient les mots-clés
filtered_articles = {keyword: articles_df[articles_df['title'].str.contains(keyword, case=False, na=False)] for keyword in keywords}

# Compter les articles pour chaque mot-clé
keyword_counts = {keyword: len(filtered_articles[keyword]) for keyword in keywords}

# Créer un DataFrame pour le graphique
keyword_counts_df = pd.DataFrame(list(keyword_counts.items()), columns=['Keyword', 'Article Count'])

# Afficher les résultats sous forme de graphique
st.subheader('Comparaison des articles par mots-clés')
plt.figure(figsize=(10, 6))
sns.barplot(x='Keyword', y='Article Count', data=keyword_counts_df, palette='viridis')
plt.xlabel('Mot-clé')
plt.ylabel('Nombre d\'articles')
plt.title('Comparaison des articles par mots-clés dans le titre')
st.pyplot(plt)




# Requête SQL pour obtenir le nombre d'articles par laboratoire
labs_article_query = """
SELECT labs.LabName, COUNT(author_article.DOI) AS article_count
FROM labs
JOIN authors ON labs.LabID = authors.LabID
JOIN author_article ON authors.AuthorID = author_article.AuthorID
GROUP BY labs.LabName
ORDER BY article_count DESC
LIMIT 10;
"""

# Charger les données avec la fonction mise à jour
labs_article_df = load_data(labs_article_query)

# Vérifiez les noms de colonnes pour confirmer qu'ils sont corrects
print(labs_article_df.columns)
st.subheader('Top 10 des laboratoires avec le plus grand nombre d\'articles')
# Afficher les résultats sous forme de graphique
plt.figure(figsize=(10, 6))
sns.barplot(x='article_count', y='labname', data=labs_article_df, palette='viridis')
plt.xlabel('Nombre d\'articles')
plt.ylabel('Laboratoire')
plt.title('Top 10 des laboratoires avec le plus grand nombre d\'articles')
st.pyplot(plt)



# Requête SQL pour obtenir le nombre d'articles par journal
journals_article_query = """
SELECT Published_In, COUNT(DOI) AS article_count
FROM articles
GROUP BY Published_In
ORDER BY article_count DESC
LIMIT 30;
"""

journals_article_df = load_data(journals_article_query)
print(journals_article_df.columns)
# Afficher les résultats
st.subheader('Top 30 des journaux avec le plus grand nombre d\'articles')
plt.figure(figsize=(10, 6))
sns.barplot(x='article_count', y='published_in', data=journals_article_df, palette='plasma')
plt.xlabel('Nombre d\'articles')
plt.ylabel('Journal')
plt.title('Top 30 des journaux avec le plus grand nombre d\'articles')
st.pyplot(plt)






keyword_type_query = """
SELECT KeywordType, COUNT(KeywordID) AS keyword_count
FROM keywords
GROUP BY KeywordType
ORDER BY keyword_count DESC;
"""
st.subheader('Répartition des types de mots-clés')
keyword_type_df = load_data(keyword_type_query)
print(keyword_type_df.columns)
plt.figure(figsize=(8, 8))
plt.pie(keyword_type_df['keyword_count'], labels=keyword_type_df['keywordtype'], autopct='%1.1f%%', startangle=140, colors=sns.color_palette("pastel"))
plt.title('Répartition des types de mots-clés')
st.pyplot(plt)


# Requête pour obtenir le top 10 des mots-clés pour chaque type
top_keywords_query = """
WITH RankedKeywords AS (
    SELECT 
        k.KeywordType, 
        k.Keyword, 
        COUNT(dk.DOI) AS keyword_count,
        ROW_NUMBER() OVER (PARTITION BY k.KeywordType ORDER BY COUNT(dk.DOI) DESC) AS rank
    FROM keywords AS k
    JOIN doi_keywords AS dk ON k.KeywordID = dk.KeywordID
    GROUP BY k.KeywordType, k.Keyword
)
SELECT KeywordType, Keyword, keyword_count
FROM RankedKeywords
WHERE rank <= 10
ORDER BY KeywordType, keyword_count DESC;
"""

# Chargement des données
top_keywords_df = load_data(top_keywords_query)
print(top_keywords_df.columns)

# Affichage
st.subheader('Top 10 des mots-clés par type')

# Création du graphique
plt.figure(figsize=(12, 8))
sns.barplot(
    data=top_keywords_df, 
    x='keyword_count', 
    y='keyword', 
    hue='keywordtype', 
    dodge=False, 
    palette='viridis'
)
plt.xlabel('Nombre d\'occurrences')
plt.ylabel('Mots-clés')
plt.title('Top 10 des mots-clés par type')
plt.legend(title='Type de mot-clé')
st.pyplot(plt)