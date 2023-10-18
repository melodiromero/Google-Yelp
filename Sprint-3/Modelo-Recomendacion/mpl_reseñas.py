
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

# Especifica la ruta del archivo Parquet en Google Cloud Storage
# ruta_del_archivo_gcs = 'gs://top-vial-398602/Output-tables/Tablas_consolidadas_anuales/2021.parquet'

# Carga el archivo Parquet desde Google Cloud Storage en un DataFrame de Pandas - Datos consolidados
dataset = pd.read_parquet('Output-tables_Tablas_consolidadas_anuales_2022.parquet')

dataset = dataset[(dataset['rating'] == 5) & (dataset['text'] != '') & (dataset['num_of_reviews'] > 2000)] # Recorte de datos

# dataset.head()

# dataset.shape

columnas_deseadas = ['business_id', 'rating', 'text', 'name', 'address', 'latitude', 'longitude','postal_code', 'ex_categories', 'state_name', 'city']

# Selecciona las columnas deseadas
df = dataset[columnas_deseadas]

# df.shape

df = df[df['text'] != '']

df = df.drop_duplicates()

# df.shape

# df.head()

# df.info()


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
# dataset.tail(50)

dataset_stem=pd.concat([dataset.text_stem, dataset.ex_categories],axis=1)
dataset_stem.dropna(axis=0,inplace=True)  # Por si quedaron titulares vacios

# dataset_stem.info()

# dataset_stem

def get_recommendations_by_terms(query_terms, min_similarity=0.3, max_similarity=1.0):
    recommendations = []
    
    for index, row in dataset_stem.iterrows():
        content_terms = set(row['text_stem'].split())  # Suponiendo que 'content' contiene las palabras clave del contenido

        # Calcula la similitud de Jaccard entre los términos de consulta y los términos del contenido
        jaccard_similarity = len(query_terms.intersection(content_terms)) / len(query_terms.union(content_terms))

        if min_similarity <= jaccard_similarity <= max_similarity:
            recommendations.append((index, jaccard_similarity))

    # Ordena por similitud descendente
    recommendations.sort(key=lambda x: x[1], reverse=True)
    
    # Inicializa un conjunto para almacenar registros únicos
    unique_records = set()
    # Inicializa un DataFrame vacío
    data = []
    
    for index, similarity in recommendations:
        element = df.loc[index, ['state_name', 'latitude', 'longitude', 'name','address','ex_categories']]

        element['similarity'] = similarity

        # Comprueba si el registro es único
        if tuple(element) not in unique_records:
            # Agregar los datos relevantes a la lista de datos
            data.append(element)

            # Agrega el registro al conjunto de registros únicos
            unique_records.add(tuple(element))

        

    # Crear un DataFrame a partir de la lista de datos
    result_df = pd.DataFrame(data)

    return result_df

# Ejemplo de uso
# query_terms = {'vegan','burger'}
# recommendations = get_recommendations_by_terms(query_terms)

# recommendations

# element = dataset_stem.loc[1266345]
# print(element)

#for index, similarity in recommendations:
#    element = df.loc[index]
#    print(f"Index: {index}, Similarity: {similarity}")
#    # Muestra otros datos específicos del elemento si es necesario
#    print(element)

def get_non_related_records(query_terms, min=0.1, threshold=0.2):
    non_related_records = []
   
    for index, row in dataset_stem.iterrows():
        content_terms = set(row['text_stem'].split())  # Suponiendo que 'content' contiene las palabras clave del contenido

        # Calcula la similitud de Jaccard entre los términos de consulta y los términos del contenido
        jaccard_similarity = len(query_terms.intersection(content_terms)) / len(query_terms.union(content_terms))

        if min <= jaccard_similarity <= threshold:
            non_related_records.append((index, jaccard_similarity))

    # Ordena por similitud ascendente (menor similitud primero)
    non_related_records.sort(key=lambda x: x[1])

    # Inicializa un conjunto para almacenar registros únicos
    unique_records = set()
    # Inicializa un DataFrame vacío
    data = []
    
    for index, similarity in non_related_records:
        element = df.loc[index, ['state_name', 'latitude', 'longitude']]

        element['similarity'] = similarity
       
        # Comprueba si el registro es único
        if tuple(element) not in unique_records:
            # Agregar los datos relevantes a la lista de datos
            data.append(element)

            # Agrega el registro al conjunto de registros únicos
            unique_records.add(tuple(element))

    # Crear result_dfun DataFrame a partir de la lista de datos
    result_df = pd.DataFrame(data)

    return result_df

# Ejemplo de uso
# query_terms_o = {'vegan','burger'}
# recommendations_o = get_non_related_records(query_terms)

# len(recommendations_o)
#
#for index, similarity in recommendations_o:
#    element = df.loc[index]
#   print(f"Index: {index}, Similarity: {similarity}")
#    # Muestra otros datos específicos del elemento si es necesario
##    print(element)