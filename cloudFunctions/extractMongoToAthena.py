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


from load.mongo import load_mongo_collection
from process.mongo import process_mongo_collection
from extract.mongo import extract_mongodb
from util.slack import send_slack_alert


MONGO_CONNECTION_STRING = os.environ.get('mongo_connection_string', 'Specified environment variable is not set.')
MONGO_DBNAME = os.environ.get('mongo_dbname', 'Specified environment variable is not set.')
BUCKET_NAME = os.environ.get('bucket_name', 'Specified environment variable is not set.')
PROJECTNAME_DATASET = os.environ.get('prjctname_dataset', 'Specified environment variable is not set.')

def start_mongo_pipeline(event=None, context=None):
    try:
        #extraction starts
        print('Extraction Started...')
        dataframe, collections = extract_mongodb(MONGO_CONNECTION_STRING, MONGO_DBNAME)
        print("Collections Received " + ', '.join(collections))
        print('Extraction Completed...')
        
        
        for collection in collections:     
            try:
                #processessing starts
                print('Processing Started for ' + str(collection))
                processed_collection = process_mongo_collection(dataframe, collection)
                print('Processing Completed for' + str(collection))
                
                #loading starts
                print('Loading Started for ' + str(collection))
                load_mongo_collection(collection, BUCKET_NAME, processed_collection, PROJECTNAME_DATASET)
                print('Loaded '+ str (collection))
            except Exception as e:
                print(e)
        
    except Exception as e:
        print(e)
        send_slack_alert(e)
