import streamlit as st
from PIL import Image

st.title("Especificaciones Técnicas del Algoritmo")

image = Image.open("../../imagenes/Sprint-3/flujo_ml.png")
st.image(image, caption="Flujo secuencia de trabajo del modelo de recomendación", use_column_width=True , width=100)

st.write("Nuestro algoritmo utiliza procesamiento de lenguaje natural (NLP) y la similitud de Jaccard, veamos como funciona tecnicamente:")

st.write("1. Define los términos clave de interés: identifica los términos clave o palabras clave que son relevantes para las recomendaciones. Estos términos clave pueden estar relacionados con atributos o características específicas del contenido de las reseñas en nuestro datawarehouse")

st.write("2. Se calcula la similitud de términos clave: utiliza técnicas de coincidencia de cadenas o similitud de términos para calcular la similitud entre los términos clave de consulta y los términos clave de las reseñas de nuestra base de datos. El métodos para calcular la similitud es la similitud de Jaccard.")

st.write("3. Clasifica por similitud: ordena los elementos en tu conjunto de datos según su similitud con los términos clave de consulta.")

st.write("4. Selecciona y presentar recomendaciones: selecciona los elementos mejor clasificados y los presenta como recomendaciones al usuario.")