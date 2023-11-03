from cloudFunctions.extractMongoToAthena import start_mongo_pipeline 

def lambda_handler(event=None, context=None): 
    start_mongo_pipeline()
