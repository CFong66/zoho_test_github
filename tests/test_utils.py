# test_utils.py
import pytest
from datetime import datetime
from unittest.mock import patch, MagicMock, mock_open
from utils import save_log_to_s3, get_zoho_secret, test_get_access_token, test_download_ca_certificate, test_get_leads_collection
from constants import *

# Mock the S3 client interaction for save_log_to_s3 function
@patch('config.boto3.client')
def test_save_log_to_s3(mock_boto_client):
    # Mock S3 client
    mock_s3_client = MagicMock()
    mock_boto_client.return_value = mock_s3_client
    
    # Call the function to test
    save_log_to_s3(
        stage="ETL Start", 
        status="IN_PROGRESS", 
        message="Starting ETL process"
    )
    
    # Assert that the S3 client put_object method was called
    mock_s3_client.put_object.assert_called_once_with(
        Bucket=S3_BUCKET_NAME,  # Replace with actual bucket name
        Key=S3_KEY_BACKUP_LEADS,
        Body='{"stage": "ETL Start", "status": "IN_PROGRESS", "message": "Starting ETL process"}'
    )


# Mock the Secrets Manager client interaction for get_zoho_secret
@patch('config.boto3.client')
def test_get_zoho_secret(mock_boto_client):
    # Mock Secrets Manager client
    mock_secrets_client = MagicMock()
    mock_boto_client.return_value = mock_secrets_client

    # Mock the return value of the get_secret_value API call
    mock_secrets_client.get_secret_value.return_value = {
        'SecretString': '{"client_id": "' + ZOHO_CLIENT_ID + '", "client_secret": "' + ZOHO_SECRET + '"}'
    }


    # Call the function to test
    secret = get_zoho_secret()

    # Assert that get_secret_value was called
    mock_secrets_client.get_secret_value.assert_called_once_with(
        SecretId='zoho-credentials'
    )

    # Assert the returned secret matches the mock
    assert secret == {"client_id": ZOHO_CLIENT_ID, "client_secret": ZOHO_SECRET}


# Test for fetching the access token (Mocked POST request to Zoho API)
@patch('requests.post')
def test_get_access_token(mock_post):
    # Mock the response for the access token
    mock_post.return_value.status_code = 200
    mock_post.return_value.json.return_value = {"access_token": "mock_access_token"}

    # Call the function
    access_token = test_get_access_token()

    # Assert the returned access token
    assert access_token == "mock_access_token"


# Test for downloading CA certificate for MongoDB
@patch('requests.get')
@patch('builtins.open', new_callable=mock_open)
def test_download_ca_certificate(mock_file, mock_get):
    # Mock the certificate download
    mock_get.return_value.status_code = 200
    mock_get.return_value.content = b"mock_certificate_content"

    # Call the function
    test_download_ca_certificate()

    # Ensure the file is written to the correct path
    mock_file.assert_called_once_with('/tmp/global-bundle.pem', 'wb')
    mock_file().write.assert_called_once_with(b"mock_certificate_content")


# Test for getting the leads collection from MongoDB
@patch('pymongo.MongoClient')
def test_get_leads_collection(mock_mongo_client):
    # Mock the MongoDB client and its methods
    mock_db = MagicMock()
    mock_collection = MagicMock()
    mock_db.list_collection_names.return_value = ['existing_collection']
    mock_db.__getitem__.return_value = mock_collection
    mock_mongo_client.return_value = mock_db

    # Call the function
    collection = test_get_leads_collection()

    # Ensure the collection was retrieved
    assert collection == mock_collection
    mock_db.create_collection.assert_not_called()  # Collection already exists
