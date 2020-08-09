#Azure Storage Account Replication
import logging
import os
from urllib.parse import urlparse
from azure.storage.blob import BlobServiceClient, BlobProperties,BlobClient,ContentSettings
from azure.core.exceptions import AzureError
import tempfile
import json

logging.getLogger().setLevel(logging.INFO)


class ASAR: #azure storage account replication
    def __init__(self,
        source_account_connection_string,
        destination_account_connection_string):
        self._source_account_connection_string = source_account_connection_string
        self._destination_account_connection_string = destination_account_connection_string
    def repl_blob(self,sourceblob_path): # since we are replicating we only need the source
        logging.info(sourceblob_path)
        container_name = os.path.dirname(sourceblob_path)
        blob_name = os.path.basename(sourceblob_path)
        logging.info(container_name)
        logging.info(blob_name)

        blob_service_client =BlobServiceClient.from_connection_string(self._source_account_connection_string)
        
        blob_connect_client = blob_service_client.get_blob_client(container=container_name,blob = blob_name)
        props = blob_connect_client.get_blob_properties()
       
        contentType = props['content_settings']['content_type']
        logging.info("content type %s",contentType)
        blob_service_clientd =BlobServiceClient.from_connection_string(self._destination_account_connection_string)
        blob_connect_clientd = blob_service_clientd.get_blob_client(container=container_name,blob = blob_name)
        
        

        my_content_settings = ContentSettings(content_type=contentType)# need to read source blob
        #if blob exist then delete
        with tempfile.TemporaryFile() as f:
            data = blob_connect_client.download_blob()
            data.readinto(f)
            f.seek(0)
            blob_connect_clientd.upload_blob(f, blob_type="BlockBlob",overwrite=True, content_settings=my_content_settings)
 
    def delete_blob(self,sourceblob_path): # since we are replicating we only need the source
        logging.info("Delete Blob")
        logging.info(sourceblob_path)
        container_name = os.path.dirname(sourceblob_path)
        blob_name = os.path.basename(sourceblob_path)
        logging.info(container_name)
        logging.info(blob_name)
        blob_service_clientd =BlobServiceClient.from_connection_string(self._destination_account_connection_string)
        blob_connect_clientd = blob_service_clientd.get_blob_client(container=container_name,blob = blob_name)
        #if blob exist then delete
        
        blob_connect_clientd.delete_blob(delete_snapshots="include")
        
        logging.info("END:DELETE_BLOB ")

def main():
    logging.info("Testing ...")
    logging.info("copy settings-template.json to settings.json and enter required connections strings")

    with open('settings.json', 'r') as openfile:
        # Reading from json file 
        settings = json.load(openfile) 
   
    source_cs = settings['source_cs']
    destination_cs = settings['destination_cs']

    logging.info("Source Connection String : %s",source_cs)
    logging.info("Destination Connection String : %s",destination_cs)

    #Enter your source blob location
    src_blob = "files/testfile1.txt"
    logging.info("Copying Blob ...")
    #Copy Blob
    bc = ASAR(source_cs,destination_cs)
    bc.repl_blob(src_blob)
    logging.info("Check to see if blob in in the destination!")
    #Wait for a key press then delete blob

    input("Press Enter to Delete")
    bc.delete_blob(src_blob)
    logging.info("Check to see if blob ihas been deleted from the destination!")


if __name__ == '__main__':
    main()

