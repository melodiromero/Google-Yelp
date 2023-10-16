import pandas as pd
from dask.dataframe import from_pandas
import os

import pyarrow as pa
from pyarrow import csv

import psycopg2

import time


def google_maps_and_metadata_merged_reviews():

    ##### Google Reviews ETL  #####

    st = time.time()

    print("Google Reviews ETL starts")
    print("reading files")

    directorio_google_reviews = F"/opt/airflow/google_maps/Python_Datasets_gmpas_reviews.parquet"
    google_reviews_df = pd.read_parquet(directorio_google_reviews ,engine="pyarrow", dtype_backend="pyarrow")
    
    google_reviews_df["time"] = pd.to_datetime(google_reviews_df["time"], unit="ms")
    google_reviews_df["time"].convert_dtypes(dtype_backend="pyarrow")
    google_reviews_df.drop(["pics", "resp"], axis=1, inplace=True)

    google_reviews_df["year"] = google_reviews_df["time"].dt.year
    google_reviews_df["year"] = google_reviews_df["year"].convert_dtypes(dtype_backend="pyarrow")

    chunk_size = 9000000

    chunks = [google_reviews_df[idx_pos: idx_pos + chunk_size] for idx_pos in range(0, len(google_reviews_df), chunk_size)]

    result_df = pd.DataFrame()

    print("filtering years")

    for chunk in chunks:
        chunk = chunk[chunk["year"] > 2011]
        result_df = pd.concat([result_df, chunk], ignore_index=True)

    chunks = None

    google_reviews_df = result_df

    

    ##### Metadata ETL #####

    print("Metadata ETL starts")
    print("reading files")

    directorio_metadata_reviews =  F"/opt/airflow/google_maps/Python_gmaps_metada.parquet"

    metada_reviews_df = pd.read_parquet(directorio_metadata_reviews).convert_dtypes(dtype_backend="pyarrow")
    metada_reviews_df.drop(["description", "price", "url", "relative_results"], axis=1, inplace=True)
    metada_reviews_df = metada_reviews_df[~metada_reviews_df["address"].isna()]
    metada_reviews_df["us_state"] = (
                                metada_reviews_df["address"]
                                # Get address element
                                .apply(lambda row: [address_element.strip() for address_element in row.split(",")][-1])
                                # Get the element of City
                                .apply(lambda x: x.split()[0])
                                )
    
    usps_state_abbreviations = {
                                'AL': 'Alabama',
                                'AK': 'Alaska',
                                'AZ': 'Arizona',
                                'AR': 'Arkansas',
                                'CA': 'California',
                                'CO': 'Colorado',
                                'CT': 'Connecticut',
                                'DE': 'Delaware',
                                'FL': 'Florida',
                                'GA': 'Georgia',
                                'HI': 'Hawaii',
                                'ID': 'Idaho',
                                'IL': 'Illinois',
                                'IN': 'Indiana',
                                'IA': 'Iowa',
                                'KS': 'Kansas',
                                'KY': 'Kentucky',
                                'LA': 'Louisiana',
                                'ME': 'Maine',
                                'MD': 'Maryland',
                                'MA': 'Massachusetts',
                                'MI': 'Michigan',
                                'MN': 'Minnesota',
                                'MS': 'Mississippi',
                                'MO': 'Missouri',
                                'MT': 'Montana',
                                'NE': 'Nebraska',
                                'NV': 'Nevada',
                                'NH': 'New Hampshire',
                                'NJ': 'New Jersey',
                                'NM': 'New Mexico',
                                'NY': 'New York',
                                'NC': 'North Carolina',
                                'ND': 'North Dakota',
                                'OH': 'Ohio',
                                'OK': 'Oklahoma',
                                'OR': 'Oregon',
                                'PA': 'Pennsylvania',
                                'RI': 'Rhode Island',
                                'SC': 'South Carolina',
                                'SD': 'South Dakota',
                                'TN': 'Tennessee',
                                'TX': 'Texas',
                                'UT': 'Utah',
                                'VT': 'Vermont',
                                'VA': 'Virginia',
                                'WA': 'Washington',
                                'WV': 'West Virginia',
                                'WI': 'Wisconsin',
                                'WY': 'Wyoming'
                            }

    us_states_set = set(list(usps_state_abbreviations.keys()))

    metada_reviews_df = metada_reviews_df[metada_reviews_df["us_state"].apply(lambda state: state in us_states_set)]

    metada_reviews_df.dropna(subset="category", axis=0, inplace=True)

    set_categories = {"Restaurant", "Bar", "bakery", "seafood", "Fast food restaurant", "Cafe", "Hamburger Restaurant",
                  "Family Restaurant","ice cream shop", "Chinese restaurant", "Italian restaurant", "Vegetarian Restaurant"
                   "Japanese restaurant", "Bistro", "Mexican restaurant", "Grill", "Dessert Restaurant"
                   "Greek restaurant", "Thai restaurant", "Indian restaurant", "Spanish restaurant",
                   "British restaurant", "Tex-mex restaurant", "American restaurant",
                   "Health food restaurant", "Delivery restaurant", "Brunch restaurant",
                   "Sandwich shop", "Fast food restaurant", "Cake shop", "Chicken restaurant",
                   "Donut shop", "Coffee shop", "Southern restaurant (US)", "Juice shop", "Breakfast restaurant",
                   "Pizza restaurant", "Bar & grill, Burrito restaurant", "Pizza", "Diner", "Takeout restaurant",
                   "Sushi restaurant", "Hamburger restaurant", "Steak house", "Asian restaurant", "New American restaurant", "Singaporean Restaurant"}

    set_categories = set([category.lower() for category in set_categories])

    mask_categories = metada_reviews_df["category"].apply(lambda categories: [category.lower().strip() in set_categories for category in categories].count(True) > 0)

    metada_reviews_df = metada_reviews_df[mask_categories]



    ##### Mergin Metadata and Reviews with Dask #####

    print("Mergin datasets")

    metadata_left_ddf = from_pandas(metada_reviews_df, npartitions=10)
    google_reviews_right_ddf = from_pandas(google_reviews_df, npartitions=10)

    result_ddf = metadata_left_ddf.merge(google_reviews_right_ddf, on="gmap_id", how="left")

    result_ddf = result_ddf.compute()




    ##### Output File #####

    print("Writting csv file")

    table = pa.Table.from_pandas(result_ddf)

    csv_file_path = F"/opt/airflow/temp_data/google_reviews.csv"

    # escapechar = "\\"

    # google_reviews_df.to_csv("C:\\Users\\jdieg\\Desktop\\henry\\proyectos\\Google-Yelp\\.data\\google_reviews.csv", escapechar=escapechar, index=False)
    
    csv.write_csv(table, csv_file_path) 

    # get the end time
    et = time.time()

    # get the execution time
    elapsed_time = et - st
    print('Execution time:', elapsed_time, 'seconds')
    

def load_data_to_postgres():

    dbname = "test_load"
    user = "postgres"
    password = "2050"
    host = "host.docker.internal"
    port = "5432"

    conn = psycopg2.connect(dbname=dbname,
                            user=user,
                            password=password,
                            host=host,
                            port=port)
    
    cursor = conn.cursor()
    # Define the PostgreSQL table name
    table_name = "metadata_google_reviews"

    # Define the path to the CSV file
    csv_file_path = F"/opt/airflow/temp_data/google_reviews.csv"

    # Define the SQL statement to copy data from the CSV file to the PostgreSQL table
    sql_statement = f"""
                    COPY {table_name} FROM stdin WITH CSV HEADER
                    DELIMITER as ','
                    """

    try:
        # Open the CSV file and copy the data to the PostgreSQL table
        with open(csv_file_path, 'r') as f:
            cursor.copy_expert(sql_statement, f)
        
        # Commit the changes
        conn.commit()
        cursor.close()
    except Exception as e:
        # Roll back the transaction in case of an error
        conn.rollback()
        cursor.close()
        conn.close()
        raise e
    finally:
        # Close the connection
        conn.close()


def erase_archive():
    file_path = "/opt/airflow/temp_data/google_reviews.csv"  # Replace with the path to the file you want to delete

    try:
        os.remove(file_path)
        print(f"{file_path} has been deleted.")
    except FileNotFoundError:
        print(f"{file_path} does not exist.")
    except Exception as e:
        print(f"An error occurred: {e}")
