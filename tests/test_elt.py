# test_etl.py
import pytest
from unittest.mock import patch, MagicMock
from etl import main

# Mock all utility functions used in the main ETL flow
@patch('src.etl.save_log_to_s3')
@patch('src.etl.fetch_leads')
@patch('src.etl.incremental_load')
@patch('src.etl.backup_mongo_data_to_s3')
@patch('src.etl.validate_data')
def test_etl_process(mock_validate_data, mock_backup_mongo_data_to_s3, mock_incremental_load, mock_fetch_leads, mock_save_log_to_s3):
    # Mock return values for each function
    mock_fetch_leads.return_value = [{'lead_id': 1, 'name': 'Test Lead'}]  # Example lead data
    mock_validate_data.return_value = None  # Assuming no return value for validation
    mock_incremental_load.return_value = None
    mock_backup_mongo_data_to_s3.return_value = None
    mock_save_log_to_s3.return_value = None

    # Call the main ETL function
    main()

    # Check that the log to S3 was called at the start
    mock_save_log_to_s3.assert_any_call(
        stage="ETL Start", 
        status="IN_PROGRESS", 
        message="Starting ETL process"
    )

    # Check that each step was called
    mock_fetch_leads.assert_called_once_with(4000)  # Adjust based on NUM_FETCH_DATA in your actual code
    mock_incremental_load.assert_called_once_with([{'lead_id': 1, 'name': 'Test Lead'}])
    mock_backup_mongo_data_to_s3.assert_called_once()
    mock_validate_data.assert_called_once()

    # Check that log to S3 was called at the end
    mock_save_log_to_s3.assert_any_call(
        stage="ETL Success", 
        status="SUCCESS", 
        message="ETL process completed successfully"
    )


# Mock for failure case
@patch('src.etl.save_log_to_s3')
@patch('src.etl.fetch_leads')
@patch('src.etl.incremental_load')
@patch('src.etl.backup_mongo_data_to_s3')
@patch('src.etl.validate_data')
def test_etl_failure(mock_validate_data, mock_backup_mongo_data_to_s3, mock_incremental_load, mock_fetch_leads, mock_save_log_to_s3):
    # Mock fetch_leads to raise an exception (simulate a failure)
    mock_fetch_leads.side_effect = Exception('Fetch error')

    # Call the main ETL function and check for exceptions
    with pytest.raises(Exception):
        main()

    # Assert the failure log is written to S3
    mock_save_log_to_s3.assert_any_call(
        stage="ETL Failure", 
        status="ERROR", 
        message="ETL process failed",
        error_message='Fetch error'
    )
