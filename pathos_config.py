#!/usr/bin/env python
# coding: utf-8

# In[1]:


from io import StringIO
from sqlalchemy import create_engine
import datetime
from sqlalchemy import Column, Integer, DateTime
import pandas as pd
from datetime import datetime 
import numpy as np
import os
pd.set_option('display.float_format', lambda x: '%.3f' % x) #to supress scientific notation
pd.set_option('display.max_rows', 500)
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)

import psycopg2 as pg
import pandas.io.sql as psql
connection = pg.connect('postgresql://aidatabases:Aidatabases#@65.1.96.15:5432/pathos_db')
engine = create_engine('postgresql://aidatabases:Aidatabases#@65.1.96.15:5432/pathos_db')

def load_gsheet(sheet_name,df_name, sheet_id):
    sheet_url = "https://docs.google.com/spreadsheets/d/{}/gviz/tq?tqx=out:csv&sheet={}".format(sheet_id, sheet_name)
    df_name = pd.read_csv(sheet_url)
    return df_name

def load_data(df_name,schema, connection):
    df = psql.read_sql('SELECT * FROM '+ schema + '.' + df_name, connection)
    return df

def truncate_table(table_name, schema):
    engine.execute('TRUNCATE {}.{} RESTART IDENTITY;'.format(schema, table_name))

def push_table_pgres(df, df_name, schema):
    df.to_sql(df_name, con=engine, index=False, if_exists= 'append', schema=schema)

def active_filter(df_name):
    df = df_name.loc[df_name['dim_active_reporting']=='Y'] 
    return df

ref_table_list = ['pathos_ref_emotions','pathos_ref_sectors', 'pathos_ref_drivers', 'pathos_ref_drivers_sectors_mapping',
                  'pathos_ref_clients', 'pathos_ref_release_version_table']

dim_table_list_rb = ['pathos_cl_master_dim_mapping', 'pathos_cl_master_manufacturer', 'pathos_cl_master_brand', 
                  'pathos_cl_master_prd_ver','pathos_cl_master_time', 'pathos_cl_master_product', 'pathos_cl_master_gmo', 
                  'pathos_cl_master_source','pathos_cl_master_channel', 'pathos_cl_master_country', 'pathos_cl_master_personas']

dim_table_list_iccs = ['pathos_cl_master_dim_mapping', 'pathos_cl_master_time','pathos_cl_master_prod_serv_cf7',
                       'pathos_cl_master_prod_serv_cf8',
                      'pathos_cl_master_channel', 'pathos_cl_master_age', 'pathos_cl_master_gender', 'pathos_cl_master_income',
                      'pathos_cl_master_occupation', 'pathos_cl_master_country', 'pathos_cl_master_personas']
               
# https://docs.google.com/spreadsheets/d/1rEDWFoQgMTtbX569at4yVoqCZ7P1iHhB --rb
# https://docs.google.com/spreadsheets/d/1X2FuqINqk8YiTf5eGCSBnrtQVdds9orIsZO_LaKBsBQ --reference
sheet_id_ref = '1X2FuqINqk8YiTf5eGCSBnrtQVdds9orIsZO_LaKBsBQ'
sheet_id_rb = '1rEDWFoQgMTtbX569at4yVoqCZ7P1iHhB'
sheet_id_iccs = '1K60gTPi9osl3mpiZIP7KN5C3mtEvWDH7'

# sheets_name_ref = ['pathos_ref_emotions','pathos_ref_sectors', 'pathos_ref_drivers', 'pathos_ref_drivers_sectors_mapping',
#                   'pathos_ref_personas', 'pathos_ref_clients', 'pathos_ref_release_version_table']
# df_name_ref = ['pathos_ref_emotions','pathos_ref_sectors', 'pathos_ref_drivers', 'pathos_ref_drivers_sectors_mapping',
#                   'pathos_ref_personas', 'pathos_ref_clients', 'pathos_ref_release_version_table']

# sheets_name_rb = ['pathos_cl_master_dim_mapping', 'pathos_cl_master_manufacturer', 'pathos_cl_master_brand', 
#                   'pathos_cl_master_prd_ver','pathos_cl_master_time', 'pathos_cl_master_product', 'pathos_cl_master_gmo', 
#                   'pathos_cl_master_source','pathos_cl_master_channel', 'pathos_cl_master_country']
# df_name_rb = ['pathos_cl_master_dim_mapping', 'pathos_cl_master_manufacturer', 'pathos_cl_master_brand', 
#                   'pathos_cl_master_prd_ver','pathos_cl_master_time', 'pathos_cl_master_product', 'pathos_cl_master_gmo', 
#                   'pathos_cl_master_source','pathos_cl_master_channel', 'pathos_cl_master_country']


# In[ ]:




