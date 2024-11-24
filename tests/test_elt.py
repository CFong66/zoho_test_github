import pytest
from etl import main

# Mock all utility functions used in the main ETL flow
def test_etl_process(mocker):
    # Mock return values for each function
    mock_save_log_to_s3 = mocker.patch("etl.save_log_to_s3")
    mock_fetch_leads = mocker.patch("etl.fetch_leads", return_value=[{"lead_id": 1, "name": "Test Lead"}])
    mock_incremental_load = mocker.patch("etl.incremental_load")
    mock_backup_mongo_data_to_s3 = mocker.patch("etl.backup_mongo_data_to_s3")
    mock_validate_data = mocker.patch("etl.validate_data")

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
    mock_incremental_load.assert_called_once_with([{"lead_id": 1, "name": "Test Lead"}])
    mock_backup_mongo_data_to_s3.assert_called_once()
    mock_validate_data.assert_called_once()

    # Check that log to S3 was called at the end
    mock_save_log_to_s3.assert_any_call(
        stage="ETL Success",
        status="SUCCESS",
        message="ETL process completed successfully"
    )

def test_etl_failure(mocker):
    # Mock fetch_leads to raise an exception (simulate a failure)
    mock_save_log_to_s3 = mocker.patch("etl.save_log_to_s3")
    mocker.patch("etl.fetch_leads", side_effect=Exception("Fetch error"))

    # Call the main ETL function and check for exceptions
    with pytest.raises(Exception, match="Fetch error"):
        main()

    # Assert the failure log is written to S3
    mock_save_log_to_s3.assert_any_call(
        stage="ETL Failure",
        status="ERROR",
        message="ETL process failed",
        error_message="Fetch error"
    )
