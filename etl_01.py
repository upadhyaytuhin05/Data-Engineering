#%%
import requests
import pandas as pd
import sqlalchemy
from sqlalchemy import create_engine
import psycopg2
import time

import sqlalchemy.orm

#%%
## Function to create Engine
def engine_cr(dialect,driver,username,password,host,port,database):
    try:
        return create_engine(f"{dialect}+{driver}://{username}:{password}@{host}:{port}/{database}",isolation_level='AUTOCOMMIT')
    except sqlalchemy.exc.ProgrammingError as e:
        print(e)


#%%

## Function for extraction of data
def extract()->dict:
    """ This API extracts data from
    http://universities.hipolabs.com
    """
    api_url = 'http://universities.hipolabs.com/search?country=India'
    data = requests.get(api_url).json()
    return data

#%%

## Function for transformation
def transform(data:dict)->pd.DataFrame:
    uni_data = pd.DataFrame(data)
    states_required = ['Dehradun', 'Haryana', 'Chittoor', 'Punjab', 'Gujarat',
       'Andhra Pradesh', 'Assam', 'Tamil Nadu', 'Uttar Pradesh',
       'West Bengal', 'Delhi', 'Madhya Pradesh', 'Rajasthan', 'Karnataka',
       'Odisha', 'Jharkhand', 'Bihar', 'Kerala', 'Maharashtra',
       'Uttarakhand', 'Nagaland', 'Himachal Pradesh', 'Chhattisgarh',
       'Tripura', 'Sikkim', 'Mizoram', 'Meghalaya', 'Telangana', 'Jammu',
       'Kashmir', 'Manipur', 'Arunachal Pradesh', 'Goa', 'Puducherry',
       'Jammu and Kashmir', 'Punjab and Haryana']
    uni_data = uni_data.loc[uni_data['state-province'].isin(states_required)]
    uni_data = uni_data[uni_data['name'].str.contains('Technology')]
    uni_data['domains'] = [','.join(map(str,l)) for l in uni_data['domains']]
    uni_data['web_pages'] = [','.join(map(str,l)) for l in uni_data['web_pages']]
    uni_data.reset_index(inplace=True)
    return uni_data



# %%
## Function to load data
def load(uni_data:pd.DataFrame,engine:sqlalchemy.engine.base.Engine)->None:
    try:
        uni_data.to_sql('India_Technology_Universities',con = engine,if_exists='replace')
        print("Data load in DB UNIVERSITY_DATA done")
    except sqlalchemy.exc.ProgrammingError as e:
        print(f"Error while data load: {e}")


#%%
## Funciton to create database
def create_database(database,engine:sqlalchemy.engine.base.Engine):
    with engine.connect() as connection:
        try:
            connection.execute(sqlalchemy.text(f"""
                SELECT pg_terminate_backend(pg_stat_activity.pid)
                FROM pg_stat_activity
                WHERE pg_stat_activity.datname = '{database}'
                  AND pid <> pg_backend_pid();
            """))
            connection.execute(sqlalchemy.text(f"DROP DATABASE IF EXISTS {database}"))
                    
            connection.execute(sqlalchemy.text(f"CREATE DATABASE {database}"))
            print({f"{database} DB created"})
        except sqlalchemy.exc.ProgrammingError as e:
            print(f"Error occured : {e}")
    

#%% 
## Creating connection with database

engine = engine_cr(dialect='postgresql',driver='psycopg2',username='postgres',password='root',host='localhost',port='5432',database='postgres')
create_database('UNIVERSITY_DATABASE',engine)

# delay of 5 sec to make sure the db is active
time.sleep(5)

retry_count=5
retry_counter = 0
while retry_counter<retry_count:
    print(f"{retry_counter} attempt in progress")
    try:
        re_engine = engine_cr(dialect='postgresql',driver='psycopg2',username='postgres',password='root',host='localhost',port='5432',database='university_database')
        print("Engine created")
        with re_engine.connect() as conn:
                conn.execute(sqlalchemy.text(f"""
                SELECT pg_terminate_backend(pg_stat_activity.pid)
                FROM pg_stat_activity
                WHERE pg_stat_activity.datname = 'university_database'
                  AND pid <> pg_backend_pid();
            """))
                result = conn.execute(sqlalchemy.text("SELECT 1"))
                print(f"database is now available")
                print(result.fetchone())
        break
    except sqlalchemy.exc.OperationalError as e:
        print(f"Database not available yet, retrying... {e}")
        retry_counter += 1
        time.sleep(2)


# %%
try:
    university_india = extract()
    print("Extract fnished")
except Exception as e:
    print(f"error: {e}")

try:
    load(uni_data=transform(university_india),engine=re_engine)
except Exception as e:
    print(f"error: {e}")

# %%
