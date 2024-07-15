# -*- coding: utf-8 -*-
"""
Created on Sat Jul 13 11:32:15 2024

@author: DAN
"""


import pandas as pd
import numpy as np
from sqlalchemy import create_engine, Column, Integer, String, Float, ForeignKey
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship, sessionmaker

# Create a base class for the declarative model
Base = declarative_base()

# Define the country_info table
class CountryInfo(Base):
    __tablename__ = 'country_info'
    id = Column(Integer, primary_key=True, autoincrement=True)
    country_name = Column(String, nullable=False)
    country_code = Column(String, unique=True, nullable=False)
    region = Column(String, nullable=False)
    income_group = Column(String, nullable=False)
    num_ci = Column(Integer, nullable=False)
    
    yearly_values = relationship("YearlyValue", back_populates="country_info")

# Define the indicator table
class Indicator(Base):
    __tablename__ = 'indicator'
    id = Column(Integer, primary_key=True, autoincrement=True)
    indicator_name = Column(String, nullable=False)
    indicator_code = Column(String, unique=True, nullable=False)
    topic = Column(String, nullable=False)
    
    yearly_values = relationship("YearlyValue", back_populates="indicator")

# Define the yearly_values table
class YearlyValue(Base):
    __tablename__ = 'yearly_value'
    id = Column(Integer, primary_key=True, autoincrement=True)
    year = Column(Integer, nullable=False)
    value = Column(Float, nullable=False)
    country_info_id = Column(Integer, ForeignKey('country_info.id'), nullable=False)
    indicator_id = Column(Integer, ForeignKey('indicator.id'), nullable=False)

    country_info = relationship("CountryInfo", back_populates="yearly_values")
    indicator = relationship("Indicator", back_populates="yearly_values")

# Connect to PostgreSQL
# DATABASE_URL = 'postgresql+psycopg2://username:password@hostname:port/dbname'

#conexion base de datos
#DATABASE_URL = "postgresql+psycopg2://utb_students:AVNS_OXQBajkVtAn2czuYQYe@pg-diplomado-utb-diplomado-utb-2024.c.aivencloud.com:24354/economic_kpis_utb"
engine = create_engine(DATABASE_URL)

# Create a session
Session = sessionmaker(bind=engine)
session = Session()

#CONSULTA DE PAISES CI=69330062
#ci correcto es 6933062
sql_query_paises = "select * from country_info ci where num_ci = 69330062"
df_paises = pd.read_sql(sql_query_paises, con=engine)

#mostramos los resultados
for index, row in df_paises.iterrows():
    print(row['country_name'])

#consulta de Y es el PIB
sql_query_pibs = "select country_info.id as id, country_name as pais, yearly_value.year as year, yearly_value.value as value from yearly_value inner join indicator on indicator.id = yearly_value.indicator_id inner join country_info on country_info.id = yearly_value.country_info_id where country_info.num_ci = 69330062 and yearly_value.indicator_id = 34"
df_pibs = pd.read_sql(sql_query_pibs, con=engine)

#consulta de P es la poblacion
sql_query_poblacion = "select country_info.id as id, country_name as pais, yearly_value.year as year, yearly_value.value as value from yearly_value inner join indicator on indicator.id = yearly_value.indicator_id inner join country_info on country_info.id = yearly_value.country_info_id where country_info.num_ci = 69330062 and yearly_value.indicator_id = 37"
df_poblacion = pd.read_sql(sql_query_poblacion, con=engine)

#consulta de de G Distribucion personal del ingreso
sql_query_ingreso = "select country_info.id as id, country_name as pais, yearly_value.year as year, yearly_value.value as value from yearly_value inner join indicator on indicator.id = yearly_value.indicator_id inner join country_info on country_info.id = yearly_value.country_info_id where country_info.num_ci = 69330062 and yearly_value.indicator_id = 35"
df_ingreso = pd.read_sql(sql_query_ingreso, con=engine)


#consulta de de pov Porcentaje de la prosperidad
sql_query_pov = "select country_info.id as id, country_name as pais, yearly_value.year as year, yearly_value.value as value from yearly_value inner join indicator on indicator.id = yearly_value.indicator_id inner join country_info on country_info.id = yearly_value.country_info_id where country_info.num_ci = 69330062 and yearly_value.indicator_id = 36"
df_pov = pd.read_sql(sql_query_pov, con=engine)

#creamos array de prosperidad 
table_prosperidad = {
    'id': [],
    'pais': [],
    'anio': [],
    'pib': [],
    'poblacion': [],
    'ingreso': [],
    'pov': [],
    'prosperidad': []
}

df_prosperidad = pd.DataFrame(table_prosperidad)

#agregamos 
for index, row in df_pibs.iterrows():
    df_prosperidad.loc[len(df_prosperidad.index)] = [row['id'], row['pais'], row['year'], row['value'], 0, 0, 0, 0] 

    #agregamos poblacion
for index, row in df_prosperidad.iterrows():
    indice = index
    for index, poblacion in df_poblacion.iterrows():
        if poblacion['id'] == row['id'] and poblacion['year'] == row['anio']:
            df_prosperidad.loc[indice] = [row['id'], row['pais'], row['anio'], row['pib'], poblacion['value'] , 0, 0, 0] 
            
# agregamos ingresos
for index, row in df_prosperidad.iterrows():
    indice = index
    for index, ingreso in df_ingreso.iterrows():
        if ingreso['id'] == row['id'] and ingreso['year'] == row['anio']:
            df_prosperidad.loc[indice] = [row['id'], row['pais'], row['anio'], row['pib'], row['poblacion'] , ingreso['value'], 0, 0] 

#agregamos pov al array            
for index, row in df_prosperidad.iterrows():
    indice = index
    for index, pov in df_pov.iterrows():
        if pov['id'] == row['id'] and pov['year'] == row['anio']:
            df_prosperidad.loc[indice] = [row['id'], row['pais'], row['anio'], row['pib'], row['poblacion'] , row['ingreso'], pov['value'], 0] 
 
#agregamos ips von la formula agregada 
for index, row in df_prosperidad.iterrows():
     ips = (row['pib'] / row['poblacion']) * (1 - row['ingreso']) * (1 - row['pov'])
     df_prosperidad.loc[index] = [row['id'], row['pais'], row['anio'], row['pib'], row['poblacion'] , row['ingreso'], pov['value'], ips] 
    
#insertamos el array a la base de datos 
for index, row in df_prosperidad.iterrows():
    if row['anio'] != 2000 and row['id'] != 51:
       new_yearly_value = YearlyValue(
           year=row['anio'],
           value=row['prosperidad'],
           country_info_id=row['id'],
           indicator_id=38
       )
    
    session.add(new_yearly_value)
    session.commit()
