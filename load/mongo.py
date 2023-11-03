import os
from boto3.session import Session
import glob

def upload_to_s3(dumpfolderpath: str, dest_bucket_name: str, dest_file_name: str, dest_folder: str):

    print(dumpfolderpath, dest_bucket_name, dest_file_name, dest_folder)
    session = Session(
        aws_access_key_id='AKIAVXAJ3POEKNE3FLFQ',
        aws_secret_access_key='tewjLP2rWn6LcimSq5gwKNUAXLBqQfwpN1KG++s'
    )

    # Create an S3 client
    s3 = session.resource('s3')
    bucket = s3.Bucket(dest_bucket_name)
     
    rel_paths = glob.glob(dumpfolderpath + '**', recursive=True)

    for local_file in rel_paths:   
        remote_path = "phmongo/" + dest_folder + "/" + dest_file_name
        if os.path.isfile(local_file):
            print(local_file, remote_path, "PATHS PRINTED")
            bucket.upload_file(local_file, remote_path, ExtraArgs={'ServerSideEncryption': 'AES256'})


def load_mongo_collection(clctn, bucket_name, dc_df, prjctname_dataset): 
        dumpfolderpath = "/tmp/mongodumpfolder/"
        ind_folder_path = dumpfolderpath + clctn + "/"
        #check if local folder path exists
        if not os.path.isdir(ind_folder_path):
            os.makedirs(ind_folder_path)
            
        dest_file_name = clctn + ".parquet"
        filepath =  ind_folder_path + dest_file_name
        print('Dumping collection to local storage: ' + clctn)
        #write data to parquet files in local
        dc_df.to_parquet(filepath)
        print('Finished dumping collection to local storage: ' + clctn)
            
        print('Uploading collection to S3: ' + clctn)
        #upload files to GCS
        upload_to_s3(ind_folder_path,bucket_name,dest_file_name,clctn)
        print('Finished uploading collection to AWS bucket: ' + clctn)