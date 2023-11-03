import pandas as pd
import os 
import sys

# accessing files in the parent dir 
# where the this file is present.
current = os.path.dirname(os.path.realpath(__file__))
 
# where the current directory is present.
parent = os.path.dirname(current)
 
# adding the parent directory to
# the sys.path.
sys.path.append(parent)

from config import config_mongo

def process_mongo_collection(dbname, collection):
    
    dc_df = pd.DataFrame()
        
    documents = dbname[collection]
        
    #converting single table json data into a table form using json_normalize
    dc = pd.json_normalize(documents.find())

    #getting the dict from config
    remove_columns = config_mongo.removeColumns


    #removing the columns in the config dict 
    if collection in remove_columns.keys():
            dc = dc.loc[:, ~dc.columns.isin(remove_columns[collection])]
            
    #formatting the field names  
    dc.columns = dc.columns.str.replace('fields.', '')
    dc.columns = dc.columns.str.replace(' ', '_')
    dc.columns = dc.columns.str.replace('.', '_')
    dc_df = pd.DataFrame(dc)

    #printing dimensions of the collection to verify number of rows and columns     
    print(dc_df.shape[0])
        
    if dc_df.shape[0] > 0 and dc_df.shape[1] > 1:
        # change objectid datatype columns to string
        for i in dc_df.select_dtypes(include='object').columns.tolist():
            dc_df[i] = dc_df[i].astype(str)
    
    return dc_df