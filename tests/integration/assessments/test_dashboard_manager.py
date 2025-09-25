import pytest

from .utils.profiler_extract_utils import build_mock_synapse_extract


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
