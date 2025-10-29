import pytest

from .utils.profiler_extract_utils import build_mock_synapse_extract


@pytest.fixture(scope="module")
def mock_synapse_profiler_extract():
    synapse_extract_path = build_mock_synapse_extract("mock_profiler_extract")
    return synapse_extract_path
