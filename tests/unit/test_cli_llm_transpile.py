import json
from pathlib import Path
from unittest.mock import create_autospec
from typing import cast

import pytest

from databricks.labs.blueprint.installation import MockInstallation, RootJsonValue
from databricks.labs.lakebridge import cli
from databricks.labs.lakebridge.contexts.application import ApplicationContext
from databricks.sdk import WorkspaceClient

_JOB_ID = 1234567890
_RUN_ID = 123456789


def create_switch_workspace_client_mock() -> WorkspaceClient:
    ws = create_autospec(spec=WorkspaceClient, instance=True)

    ws.config.host = 'https://workspace.databricks.com'
    ws.files.upload.return_value = None
    ws.jobs.run_now.return_value.run_id = _RUN_ID
    ws.jobs.run_now_and_wait_result.return_value.run_id = _RUN_ID

    return ws


@pytest.fixture
def mock_installation_with_switch() -> MockInstallation:
    """MockInstallation with Switch configuration state."""
    state: dict[str, RootJsonValue] = {
        "config.yml": {
            "version": 3,
            "transpiler_config_path": str(Path.home() / ".lakebridge" / "Switch" / "lsp" / "config.yml"),
            "transpiler_options": {
                "catalog": "test_catalog",
                "schema": "test_schema",
                "volume": "test_volume",
                "foundation_model": "databricks-claude-sonnet-4-5",
                "transpiler_name": "Switch",
            },
            "source_dialect": "mssql",
            "input_source": "input_sql",
            "output_folder": "output_folder",
            "sdk_config": None,
            "skip_validation": False,
            "catalog_name": "catalog",
            "schema_name": "schema",
        },
        "state.json": {"resources": {"jobs": {"Switch": f"{_JOB_ID}"}}, "version": 1},
    }
    return MockInstallation(cast(dict[str, RootJsonValue], state))


def test_llm_transpile_success(
    mock_installation_with_switch: MockInstallation,
    tmp_path: Path,
    capsys,
) -> None:
    """Test successful LLM transpile execution."""
    input_source = tmp_path / "input.sql"
    input_source.write_text("SELECT * FROM table1;")
    output_folder = "/Workspace/Users/test/output"

    # Use a dedicated WorkspaceClient mock tailored for SwitchRunner
    mock_ws = create_switch_workspace_client_mock()

    ctx = ApplicationContext(mock_ws)
    ctx.replace(installation=mock_installation_with_switch)
    ctx.replace(add_user_agent_extra=lambda w, *args, **kwargs: w)

    cli.llm_transpile(
        w=mock_ws,
        input_source=str(input_source),
        output_ws_folder=output_folder,
        source_dialect="mssql",
        ctx=ctx,
    )

    (out, _) = capsys.readouterr()
    result = json.loads(out)
    assert [
        {
            "job_id": _JOB_ID,
            "run_id": _RUN_ID,
            "run_url": f"https://workspace.databricks.com/jobs/{_JOB_ID}/runs/{_RUN_ID}",
        }
    ] == result
