
import itertools

import numpy as np
import pandas as pd

import matplotlib.pyplot as plt
import seaborn as sns
sns.set()

import nltk

# Importamos la función que nos permite Stemmizar de nltk y definimos el stemmer
from nltk.stem import PorterStemmer
stemmer = PorterStemmer()

import re

# Esto sirve para configurar NLTK. La primera vez puede tardar un poco
nltk.download('punkt')
nltk.download('stopwords')

from google.colab import auth
auth.authenticate_user()  # Esto abrirá una ventana emergente para autenticarte con tu cuenta de Google.

# Especifica la ruta del archivo Parquet en Google Cloud Storage
ruta_del_archivo_gcs = 'gs://top-vial-398602/Output-tables/Tablas_consolidadas_anuales/2021.parquet'

# Carga el archivo Parquet desde Google Cloud Storage en un DataFrame de Pandas
dataset = pd.read_parquet(ruta_del_archivo_gcs)

dataset = dataset[(dataset['rating'] == 5) & (dataset['text'] != '') & (dataset['num_of_reviews'] > 2000)] # Recorte de datos

dataset.head()

dataset.shape

columnas_deseadas = ['business_id', 'rating', 'text', 'name', 'address', 'latitude', 'longitude','postal_code', 'ex_categories', 'state_name', 'city']

# Selecciona las columnas deseadas
df = dataset[columnas_deseadas]

df.shape

df = df[df['text'] != '']

df = df.drop_duplicates()

df.shape

df.head()

df.info()



"""MUESTRA DE DATOS A UTILIZAR"""

dataset=pd.concat([df.text,df.ex_categories],axis=1)
dataset.dropna(axis=0,inplace=True)  # Si hay alguna nan, tiramos esa instancia
dataset.head()

# Traemos nuevamente las stopwords
stopwords = nltk.corpus.stopwords.words('english')

# Recorremos todos los textos y le vamos aplicando la Normalizacion y luega el Stemming a cada uno
reseña_list=[]
for reseña in dataset.text:
    # Vamos a reemplazar los caracteres que no sean leras por espacios
    reseña=re.sub("[^a-zA-Z]"," ",str(reseña))
    # Pasamos todo a minúsculas
    reseña=reseña.lower()
    # Tokenizamos para separar las palabras del titular
    reseña=nltk.word_tokenize(reseña)

    # Sacamos las Stopwords
    reseña = [palabra for palabra in reseña if not palabra in stopwords]

    ## Hasta acá Normalizamos, ahora a stemmizar

    # Aplicamos la funcion para buscar la raiz de las palabras
    reseña=[stemmer.stem(palabra) for palabra in reseña]
    # Por ultimo volvemos a unir la rreseña
    reseña=" ".join(reseña)

    # Vamos armando una lista con todos las reseñas
    reseña_list.append(reseña)

dataset["text_stem"] = reseña_list
dataset.tail(50)

dataset_stem=pd.concat([dataset.text_stem, dataset.ex_categories],axis=1)
dataset_stem.dropna(axis=0,inplace=True)  # Por si quedaron titulares vacios

dataset_stem.info()

dataset_stem

def get_recommendations_by_terms(df, query_terms, num_recommendations=10):
    recommendations = []

    for index, row in df.iterrows():
        content_terms = set(row['text_stem'].split())  # Suponiendo que 'content' contiene las palabras clave del contenido

        # Calcula la similitud de Jaccard entre los términos de consulta y los términos del contenido
        jaccard_similarity = len(query_terms.intersection(content_terms)) / len(query_terms.union(content_terms))

        recommendations.append((index, jaccard_similarity))

    # Ordena por similitud descendente
    recommendations.sort(key=lambda x: x[1], reverse=True)

    # Retorna las mejores recomendaciones
    return recommendations[:num_recommendations]

# Ejemplo de uso
query_terms = {'vegan','burger'}
recommendations = get_recommendations_by_terms(dataset_stem, query_terms)

recommendations

element = dataset_stem.loc[1266345]
print(element)

for index, similarity in recommendations:
    element = df.loc[index]
    print(f"Index: {index}, Similarity: {similarity}")
    # Muestra otros datos específicos del elemento si es necesario
    print(element)

def get_non_related_records(df, query_terms, threshold=0.2):
    non_related_records = []

    for index, row in df.iterrows():
        content_terms = set(row['text_stem'].split())  # Suponiendo que 'content' contiene las palabras clave del contenido

        # Calcula la similitud de Jaccard entre los términos de consulta y los términos del contenido
        jaccard_similarity = len(query_terms.intersection(content_terms)) / len(query_terms.union(content_terms))

        if jaccard_similarity < threshold:
            non_related_records.append((index, jaccard_similarity))

    # Ordena por similitud ascendente (menor similitud primero)
    non_related_records.sort(key=lambda x: x[1])

    return non_related_records

# Ejemplo de uso
query_terms_o = {'vegan','burger'}
recommendations_o = get_non_related_records(dataset_stem, query_terms)

len(recommendations_o)

for index, similarity in recommendations_o:
    element = df.loc[index]
    print(f"Index: {index}, Similarity: {similarity}")
    # Muestra otros datos específicos del elemento si es necesario
    print(element)