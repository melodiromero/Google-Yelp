import streamlit as st
import pandas as pd

from PIL import Image
image = Image.open("../../imagenes/Imagen-Corporativa/logo.png")
st.image(image, caption="Logo de Blue Consulting Group", use_column_width=True , width=100)
# Introducción
st.title("Detección de Oportunidades de Negocio")

st.write("Bienvenido a nuestra aplicación de detección de oportunidades de negocio para aperturas de restaurantes en los Estados Unidos. Nuestro modelo de recomendación utiliza procesamiento de lenguaje natural (NLP) y la similitud de Jaccard para ofrecer recomendaciones basadas en la competencia en diferentes ciudades.")

st.write("El objetivo de esta aplicación es ayudarte a identificar lugares con alta competencia, donde no es recomendable abrir un restaurante, así como lugares que representan oportunidades de negocio para la apertura de nuevos locales.")

st.header("¿Cómo funciona?")

st.write("Nuestro algoritmo utiliza procesamiento de lenguaje natural (NLP) para analizar los términos clave proporcionados y la similitud de Jaccard para compararlos con los términos de contenido en nuestra base de datos. La similitud de Jaccard se utiliza para determinar la relevancia de las recomendaciones.")

st.write("Nuestra base de datos se compone de una unificación de las reseñas bridadas por Google Maps y Yelp en los últimos años.")

st.header("Oportunidades de Mercado")

st.write("Mediante el análisis de las palabras claves, te recomienda los Estados que resultan oportunos para aperturar locales de la categoria o palabrs claves buscados.")

st.header("Competencias Exitosas")

st.write("Mediante el análisis de las palabras claves, te advierte en que Estados no resultaria oportuno establecerse, ya que existen competencias bien posicionadas o fuertes en estas localizaciones.")
