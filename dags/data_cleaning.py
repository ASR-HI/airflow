import pandas as pd

def clean_csv_data(authors_df, keywords_df, labs_df, doi_authors_df, doi_keywords_df):
    # Vérification et suppression des doublons pour chaque DataFrame
    for df in [authors_df, keywords_df, labs_df, doi_authors_df, doi_keywords_df]:
        df.drop_duplicates(inplace=True)

    # Assurer que la colonne 'Lab' existe dans labs_df avant de remplir les valeurs manquantes
    if 'Lab' in labs_df.columns:
        labs_df['Lab'].fillna('Non mentionné', inplace=True)

    return {
        "authors": authors_df,
        "keywords": keywords_df,
        "labs": labs_df,
        "doi_keywords": doi_keywords_df,
        "doi_authors": doi_authors_df,
    }

def clean_json_data(json_df):
    # Suppression des doublons dans le DataFrame JSON
    json_df.drop_duplicates(inplace=True)
    return json_df

import pandas as pd

# Fonction de nettoyage pour le fichier des auteurs
def clean_authors_data(authors_df):
    # Suppression des lignes avec des valeurs manquantes
    authors_df.dropna(inplace=True)
    # Suppression des doublons
    authors_df.drop_duplicates(inplace=True)
    return authors_df

# Fonction de nettoyage pour le fichier des mots-clés
def clean_keywords_data(keywords_df):
    keywords_df.dropna(inplace=True)
    keywords_df.drop_duplicates(inplace=True)

    return keywords_df

# Fonction de nettoyage pour le fichier des laboratoires
def clean_labs_data(labs_df):
    labs_df['Lab'].fillna('Non mentionné', inplace=True)
    labs_df.dropna(inplace=True)
    labs_df.drop_duplicates(inplace=True)
    return labs_df

# Fonction de nettoyage pour le fichier DOI-auteurs
def clean_doi_authors_data(doi_authors_df):
    doi_authors_df.dropna(inplace=True)
    doi_authors_df.drop_duplicates(inplace=True)

    return doi_authors_df

# Fonction de nettoyage pour le fichier DOI-mots-clés
def clean_doi_keywords_data(doi_keywords_df):
    doi_keywords_df.dropna(inplace=True)
    doi_keywords_df.drop_duplicates(inplace=True)

    return doi_keywords_df

