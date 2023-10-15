#!/usr/bin/env python
# coding: utf-8
'''
Extracción y transformacín de datos de Yelp para generar una tabla única

'''
# In[1]:
import numpy as np
#import polars as pl
import pandas as pd
import matplotlib.pyplot as plt
import pyarrow.parquet as pq
import re
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import findspark
from pyspark.sql.functions import from_unixtime, year
from pyspark.sql.functions import sum, when, col
import pyspark
import os
import pyspark.sql.functions as F
findspark.init()
os.environ["JAVA_HOME"] = "C:\Java\jdk-18.0.1.1"
spark = SparkSession.builder.appName("yelp").getOrCreate()


# In[2]:
yelp_folder_path= "D:/KDG/Henry/LABS\PF/Yelp/review.json"
yelp_business_path = "D:/KDG/Henry/LABS/PF/Yelp/business.pkl"
yelp_review_df = pd.read_json(yelp_folder_path, lines=True, engine="pyarrow", dtype_backend="pyarrow")
# In[3]:
yelp_review_df["year"] = yelp_review_df["date"].dt.year
yelp_review_df["month"] = yelp_review_df["date"].dt.month
# In[4]: Dividiendo en que momento del día se deja la reseña
time_labels = ['Night', 'Morning', 'Afternoon', 'Evening', "Night"] 
time_bins = [0, 6, 12, 18, 22, 24]
yelp_review_df["time_category"] = pd.cut(yelp_review_df["date"].dt.hour, bins=time_bins, labels=time_labels, ordered=False, include_lowest=True)
# In[5]: Rellenando valores nuelos
business = pd.read_pickle(yelp_business_path)
business["attributes"].fillna("{'Attribute': 'Unknown'}",inplace=True)
business["categories"].fillna("Unknown Category", inplace=True)
business["hours"].fillna("Unknown Hours", inplace=True)
business["state"].fillna("XX", inplace=True)
business["attributes"].iloc[:,0].fillna("{'Attribute': 'Unknown'}", inplace=True)
business["attributes"].isna().sum()
business_columns = business.columns[:14]
business.loc[business["attributes"].iloc[:,0].isna(), "attributes"].iloc[:,0] = "No attributes"
business.columns = range(len(business.columns))
business.drop(business.loc[:,14:].columns, axis=1, inplace=True)
business.columns = business_columns[:14]
# In[6]: dejando el tipo de variable correcto
business["business_id"]=business["business_id"].astype("string")
business["name"]=business["name"].astype("string")
business["address"]=business["address"].astype("string")
business["city"]=business["city"].astype("string")
business["state"]=business["state"].astype("string")
business["is_open"]=business["is_open"].astype("string")
business["hours"]=business["hours"].astype("string")
business["review_count"]=business["review_count"].astype("int")
business["stars"]=business["stars"].astype("int")
# In[6.1]:
# In[7]: dejando el tipo de variable correcto y explotando la columna categorías
business.drop(business[business["latitude"] > 53].index, inplace=True)
business.dropna(subset="attributes", inplace=True)
business["categories"] = (business["categories"].astype(str).apply(lambda x: x.split(",")).apply(lambda x: [x_.strip() for x_ in x]))
business["ex_categories"] = business["categories"]
business = business.explode('ex_categories').reset_index(drop=True)
business["ex_categories"] = business["ex_categories"].apply(lambda x: re.sub(r"['\"\[\]]", '', x))
# In[9]: Lista de categorías permitidas
categorias_permitidas = {"Restaurants", "Food","Pizza","Coffee & Tea", "Fast Food", "Breakfast & Brunch", "Burgers","Mexican","Italian","Specialty Food",
                  "Seafood", "Chinese", "Bakeries", "Salad", "Cafes", "Ice Cream & Frozen Yogurt","Delis","Japanese","Sushi Bars","Juice Bars & Smoothies",
                  "Asian Fusion","Steakhouses","Diners","Tacos","Thai","Tex-Mex","Vegan","Vietnamese","Indian","Latin American","Greek","Gluten-Free",
                  "Hot Dogs","Bagels","Ethnic Food", "Buffets","Soul Food","Korean"}
# In[10]: filtrando solo loa categorías permitidas
business_filtrado = business[business["ex_categories"].isin(categorias_permitidas)]
# In[11]: trabajando con la columna atributos
columnas_duplicadas = ["business_id", "name"]
business_filtrado = business_filtrado.drop_duplicates(subset=columnas_duplicadas)
business_filtrado = business_filtrado.drop(columns = ["stars","is_open"])

# In[12]: Se guarda como parquet para realizar la unión con gmaps
business_filtrado = business_filtrado.rename(columns={"state":"us_state","review_count":"num_of_reviews"})
output_path = "D:/KDG/Henry/LABS/PF/Yelp/business"
business_filtrado.to_parquet(output_path + ".parquet")
# In[13]: ------------USERS------------
users = pd.read_parquet("D:/KDG/Henry/LABS/PF/Yelp/user.parquet", engine= "pyarrow")
users["user_id"]=users["user_id"].astype("string")
users["name"]=users["name"].astype("string")
users["yelping_since"]=pd.to_datetime(users["yelping_since"])
users["elite"]=users["elite"].astype("string")
users["friends"]=users["friends"].astype("string")
users = users.drop(columns = users.columns[10:22])
users = users.drop(columns = ["friends","fans","cool","funny","useful","elite","yelping_since"])
users = users.rename(columns={"name":"user_name","review_count": "user_review_count"})

# In[14]: Output de users transformado
output_path = "D:/KDG/Henry/LABS/PF/Yelp/users"
users.to_parquet(output_path+".parquet")

# In[15]: -----------------REVIEWS---------------------------
reviews = spark.read.json("D:/KDG/Henry/LABS/PF/Yelp/review.json")
# In[15]: Transformación de REVIEWS
reviews = reviews.withColumn("año", year(reviews["date"]))
reviews = reviews.filter(reviews["año"] >= 2018)
reviews = reviews.drop("cool")
reviews = reviews.drop("funny")
reviews = reviews.drop("useful")
reviews = reviews.drop("review_id")
reviews = reviews.withColumnRenamed("date", "datetime") \
                 .withColumnRenamed("stars", "rating")
output_path = "D:/KDG/Henry/LABS/PF/Yelp/reviews"
reviews.to_parquet(output_path+".parquet")
# In[15]: Output de REVIEWS transformada
business = spark.read.parquet("D:/KDG/Henry/LABS/PF/Yelp/business.parquet")
users = spark.read.parquet("D:/KDG/Henry/LABS/PF/Yelp/users.parquet")
reviews = spark.read.parquet("D:/KDG/Henry/LABS/PF/Yelp/reviews.parquet")
# In[15]: Output de REVIEWS transformada
'''SE GENERA EL DATAFRAME CONSOLIDADO DE YELP TOMANDO COMO BASE LOS 3 ARCHIVOS PARQUET TRANSOFMADO
1) USERS.PARQUET
2) REVIEWS.PARQUET
3) BUSINESS.PARQUET'''

business = spark.read.parquet("D:/KDG/Henry/LABS/PF/Yelp/business.parquet")
users = spark.read.parquet("D:/KDG/Henry/LABS/PF/Yelp/users.parquet")
#SE realizar los join respectivos
yelp_reviews_users_b = reviews.join(users, on="user_id", how="inner")
yelp_reviews_users_b = yelp_reviews_users_b.join(business, on="business_id", how="inner")

#Hace input de los nombres de estado
csvFilePath = "D:/KDG/Henry/LABS/PF/Google-Yelp/Sprint-2/ETL_EDA/Google_Maps/usps_state_abbreviations.csv"
state_names = spark.read.option("header", "true").csv(csvFilePath)
yelp_reviews_users_b = yelp_reviews_users_b.join(state_names, on ="us_state",how = "inner")

#Se reorfaginzan las columnas para que concidan con las de el df de Google y se genera la columna plataforma
yelp_reviews_users_b = yelp_reviews_users_b.select("business_id","user_id","user_name","rating","text","datetime","año","us_state","name","address","latitude","longitude","categories","hours","postal_code","num_of_reviews","attributes","ex_categories","state_name","city")
yelp_reviews_users_b = yelp_reviews_users_b.withColumn("plataforma", F.lit("Yelp"))

#Output de la tabla consolidada de Yelp
output_path = "D:/KDG/Henry/LABS/PF/Yelp/yelp_reviews_users"
yelp_reviews_users_b.coalesce(1).write.option("header", "true").parquet(output_path, mode="overwrite")
