import requests
import json
from sqlalchemy import create_engine
import pandas as pd

url = "http://api.nasa.gov/neo/rest/v1/feed?start_date=2023-04-01&end_date=2023-04-06&detailed=false&api_key=n8TsUc11AeGkNMzP8c2QQEsZsmdX98hb3usc4uKs"

r = requests.get(url)

data = json.loads(r.text)

near_earth_objects = data["near_earth_objects"]

flat_list = [item for sublist in near_earth_objects.values() for item in sublist]

def formato(x):
    return {
        "id":x["id"],
        "nombre":x["name"],
        "diametro minimo":x["estimated_diameter"]["kilometers"]["estimated_diameter_min"],
        "diametro maximo":x["estimated_diameter"]["kilometers"]["estimated_diameter_max"],
        "tamaño absoluto":x["absolute_magnitude_h"],
        "peligro potencial":x["is_potentially_hazardous_asteroid"],
    }

mapped_list = list(map(formato,flat_list))

object_dict = {x["id"]: x for x in mapped_list}

url="data-engineer-cluster.cyhh5bfevlmn.us-east-1.redshift.amazonaws"
port="5439"
data_base="data-engineer-database"
user="ursomatias_coderhouse"
pwd="5356w6ZEyufT"

pd_object=pd.DataFrame.from_dict(object_dict, orient="index")

conn = create_engine(f'postgresql://{user}:{pwd}@{url}.com:5439/{data_base}')

pd_object.to_sql('NASA', conn, index=False, if_exists='replace', schema="ursomatias_coderhouse")