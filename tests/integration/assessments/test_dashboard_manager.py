import pytest

from .utils.profiler_extract_utils import build_mock_synapse_extract
import os
from unittest.mock import patch, mock_open, MagicMock
from databricks.labs.lakebridge.assessments.dashboards.dashboard_manager import DashboardManager


@pytest.fixture(scope="module")
def mock_synapse_profiler_extract():
    synapse_extract_path = build_mock_synapse_extract("mock_profiler_extract")
    return synapse_extract_path


# Step One:
# Fetch environment variables for Databricks workspace URL, token, catalog, schema, volume name
# This will be moved into CLI prompts

# Step Two:
# Test that the DuckDB file can be uploaded to a target UC Volume
# TODO: Create class/function for uploading Duck DB file

# Step Three:
# Test that the job can be deployed to Databricks workspace

# Step Four:
# Test that the dashboard can be deployed to the workspace

@pytest.fixture
def dashboard_manager():
    return DashboardManager(
        workspace_url="https://test-workspace.cloud.databricks.com",
        token="test_token",
        warehouse_id="test_warehouse_id",
        databricks_username="test_user@databricks.com"
    )

@patch("os.path.exists")
@patch("requests.put")
def test_upload_duckdb_to_uc_volume_success(mock_put, mock_exists, dashboard_manager):
    # Mock the file existence check
    mock_exists.return_value = True

    # Mock the PUT request response
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_put.return_value = mock_response

    # Mock the open function to simulate reading the file
    with patch("builtins.open", mock_open(read_data="mock_data")) as mocked_file:
        # Call the method
        result = dashboard_manager.upload_duckdb_to_uc_volume(
            workspace_url="https://test-workspace.cloud.databricks.com",
            access_token="test_token",
            local_file_path="/path/to/mock_file.duckdb",
            volume_path="/Volumes/catalog/schema/volume/mock_file.duckdb"
        )

        mocked_file.assert_called_once_with("/path/to/mock_file.duckdb", "rb")

    # Assertions
    assert result is True


@patch("os.path.exists")
def test_upload_duckdb_to_uc_volume_file_not_found(mock_exists, dashboard_manager):
    # Mock the file existence check
    mock_exists.return_value = False

    # Call the method
    result = dashboard_manager.upload_duckdb_to_uc_volume(
        workspace_url="https://test-workspace.cloud.databricks.com",
        access_token="test_token",
        local_file_path="/path/to/nonexistent_file.duckdb",
        volume_path="/Volumes/catalog/schema/volume/mock_file.duckdb"
    )

    # Assertions
    assert result is False

@patch("os.path.exists")
def test_upload_duckdb_to_uc_volume_invalid_volume_path(mock_exists, dashboard_manager):
    # Mock the file existence check
    mock_exists.return_value = True

    # Call the method with an invalid volume path
    result = dashboard_manager.upload_duckdb_to_uc_volume(
        workspace_url="https://test-workspace.cloud.databricks.com",
        access_token="test_token",
        local_file_path="/path/to/mock_file.duckdb",
        volume_path="/InvalidPath/mock_file.duckdb"
    )

    # Assertions
    assert result is False

@patch("os.path.exists")
@patch("requests.put")
def test_upload_duckdb_to_uc_volume_upload_failure(mock_put, mock_exists, dashboard_manager):
    # Mock the file existence check
    mock_exists.return_value = True

    # Mock the PUT request response
    mock_response = MagicMock()
    mock_response.status_code = 500
    mock_response.text = "Internal Server Error"
    mock_put.return_value = mock_response

    # Call the method
    result = dashboard_manager.upload_duckdb_to_uc_volume(
        workspace_url="https://test-workspace.cloud.databricks.com",
        access_token="test_token",
        local_file_path="/path/to/mock_file.duckdb",
        volume_path="/Volumes/catalog/schema/volume/mock_file.duckdb"
    )

    # Assertions
    assert result is False
