import streamlit as st
import mpl_reseñas as m

st.title("Oportunidades de Negocio")

st.markdown("***")

st.markdown("# Modelo de Recomendación")

# Widget de entrada de texto para palabras clave
keywords = st.text_input("Ingrese las palabras clave separadas por comas (por ejemplo, palabra1, palabra2):")

# Botón para buscar el dataset
if st.button("Ver oportunidades de negocio ..."):
    # Dividir las palabras clave ingresadas por comas
    keywords_list = [keyword.strip() for keyword in keywords.split(",")]

    # Obtener el dataset basado en las palabras clave
    dataset = m.get_non_related_records(keywords_list)

    # Mostrar el dataset resultante
    st.write("Dataset Resultante:")
    st.dataframe(dataset)
