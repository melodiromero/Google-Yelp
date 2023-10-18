import streamlit as st
import mpl_reseñas as m
import folium
import pandas as pd

st.title("Modelo de Recomendación")

st.markdown("***")

st.sidebar.markdown("Modelo de Recomendación")

# Widget de entrada de texto para palabras clave
keywords = st.text_input("Ingrese las palabras clave separadas por comas (por ejemplo, palabra1, palabra2):")

keywords_list =  set(keyword.strip() for keyword in keywords.split(','))

# Validar si se ingresó texto antes de permitir que el usuario presione los botones
if keywords.strip() != "":

    # Botón para buscar el dataset
    if st.button("OPORTUNIDADES DE MERCADO"):
        # Dividir las palabras clave ingresadas por comas
        
        # Obtener el dataset basado en las palabras clave
        dataset_oportunidades = m.get_non_related_records(keywords_list)

        st.write("Potenciales lugares de apertura de restaurantes")

        # Seleccionar las columnas deseadas y mostrar el DataFrame en Streamlit
        st.dataframe(dataset_oportunidades)
        st.write("")

    

    # Botón para buscar el dataset
    if st.button("COMPETENCIAS EXITOSAS"):
       
        # Obtener el dataset basado en las palabras clave
        dataset_competencias = m.get_recommendations_by_terms(keywords_list)

        st.write("Resultados de estados donde ya existe competencia y no recomendamos aperturar en los mismos.")

        # Seleccionar las columnas deseadas y mostrar el DataFrame en Streamlit
        st.dataframe(dataset_competencias)

else:
    st.warning("Por favor, ingrese al menos una palabra antes de buscar recomendaciones.")