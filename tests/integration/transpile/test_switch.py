import os
from dataclasses import dataclass
from pathlib import Path
from typing import cast

import pytest
from databricks.labs.blueprint.installation import Installation, RootJsonValue
from databricks.labs.blueprint.installer import InstallState
from databricks.labs.blueprint.wheels import ProductInfo
from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import NotFound

from databricks.labs.lakebridge.config import SwitchResourcesConfig
from databricks.labs.lakebridge.contexts.application import ApplicationContext
from databricks.labs.lakebridge.deployment.job import JobDeployment
from databricks.labs.lakebridge.deployment.switch import SwitchDeployment
from databricks.labs.lakebridge.transpiler.installers import SwitchInstaller
from databricks.labs.lakebridge.transpiler.repository import TranspilerRepository
from databricks.labs.lakebridge.transpiler.switch_runner import SwitchConfig, SwitchRunner


@dataclass
class SwitchSetup:
    """Setup results for Switch installation and deployment."""

    installation: Installation
    switch_deployment: SwitchDeployment
    job_id: int


def test_switch_install(ws: WorkspaceClient, switch_artifact: Path):
    """Test Switch install: installation, job deployment, and resource verification."""
    setup = _setup_switch(ws, switch_artifact, "test_catalog", "test_schema", "test_volume")

    try:
        install_state = InstallState.from_installation(setup.installation)
        _verify_resource_persistence(install_state)
        _verify_job_id_retrieval(install_state, setup.job_id)
    finally:
        _cleanup_switch(setup.switch_deployment, setup.installation)


def test_switch_install_with_transpile(ws: WorkspaceClient, switch_artifact: Path):
    """Test Switch install with transpile: full workflow including job execution and output verification."""
    if os.getenv("LAKEBRIDGE_SWITCH_E2E") != "true":
        pytest.skip("E2E test requires LAKEBRIDGE_SWITCH_E2E=true")

    e2e_catalog = os.getenv("LAKEBRIDGE_SWITCH_E2E_CATALOG", "main")
    e2e_schema = os.getenv("LAKEBRIDGE_SWITCH_E2E_SCHEMA", "lakebridge_switch_e2e")
    e2e_volume = os.getenv("LAKEBRIDGE_SWITCH_E2E_VOLUME", "switch_volume")
    setup = _setup_switch(ws, switch_artifact, e2e_catalog, e2e_schema, e2e_volume)

    try:
        current_user = ws.current_user.me().user_name
        output_folder = f"/Workspace/Users/{current_user}/switch_e2e_output"
        run_results = _run_switch_transpile(
            ws, setup.installation, output_folder, e2e_catalog, e2e_schema, e2e_volume, setup.job_id
        )
        _verify_transpile_results(run_results, setup.job_id)
        _verify_output_notebook(ws, f"{output_folder}/test_ctas_simple.py")

    finally:
        _cleanup_switch(setup.switch_deployment, setup.installation)


def _setup_switch(ws: WorkspaceClient, switch_artifact: Path, catalog: str, schema: str, volume: str) -> SwitchSetup:
    """Setup Switch: local installation, workspace deployment, and job creation."""
    context = ApplicationContext(ws)
    installation = context.installation
    install_state = InstallState.from_installation(installation)
    transpiler_repository = TranspilerRepository.user_home()

    # Local installation
    installer = SwitchInstaller(transpiler_repository)
    installer.install(switch_artifact)

    # Workspace deployment
    product_info = ProductInfo.from_class(SwitchDeployment)
    job_deployer = JobDeployment(ws, installation, install_state, product_info)
    switch_deployment = SwitchDeployment(
        ws, installation, install_state, product_info, job_deployer, transpiler_repository
    )

    resources = SwitchResourcesConfig(catalog=catalog, schema=schema, volume=volume)
    switch_deployment.install(resources)

    # Verify job creation
    install_state = InstallState.from_installation(installation)
    job_id = _get_and_verify_job_id(ws, install_state)

    return SwitchSetup(installation=installation, switch_deployment=switch_deployment, job_id=job_id)


def _run_switch_transpile(
    ws: WorkspaceClient,
    installation: Installation,
    output_folder: str,
    catalog: str,
    schema: str,
    volume: str,
    job_id: int,
) -> RootJsonValue:
    """Run Switch transpile job with test SQL and return results."""
    test_sql_path = (
        Path(__file__).parent.parent.parent / "resources" / "functional" / "snowflake" / "ddl" / "test_ctas_simple.sql"
    )

    runner = SwitchRunner(ws, installation)
    return runner.run(
        input_source=str(test_sql_path),
        output_ws_folder=output_folder,
        source_dialect="snowflake",
        catalog=catalog,
        schema=schema,
        volume=volume,
        job_id=job_id,
        switch_options={},
        wait_for_completion=True,
    )


def _verify_transpile_results(run_results: RootJsonValue, expected_job_id: int) -> None:
    """Verify Switch transpile job execution results."""
    assert isinstance(run_results, list)
    assert len(run_results) == 1
    result = cast(dict, run_results[0])
    assert result.get("job_id") == expected_job_id
    assert result.get("state") == "TERMINATED"
    assert result.get("result_state") == "SUCCESS"


def _verify_output_notebook(ws: WorkspaceClient, output_path: str) -> None:
    """Verify output notebook creation."""
    notebook_info = ws.workspace.get_status(output_path)
    assert notebook_info.object_type is not None
    assert notebook_info.object_type.value == "NOTEBOOK"


def _get_and_verify_job_id(ws: WorkspaceClient, install_state: InstallState):
    """Get job ID and verify job creation and registration."""
    assert "Switch" in install_state.jobs
    job_id = int(install_state.jobs["Switch"])

    job = ws.jobs.get(job_id)
    assert job is not None
    assert job.settings is not None
    assert job.settings.name is not None
    assert "switch" in job.settings.name.lower()

    assert job.settings.tasks is not None
    assert len(job.settings.tasks) > 0
    task = job.settings.tasks[0]
    assert task.notebook_task is not None
    assert "switch" in task.notebook_task.notebook_path.lower()
    return job_id


def _verify_resource_persistence(install_state: InstallState):
    """Verify resource persistence."""
    assert install_state.switch_resources is not None
    resources = install_state.switch_resources
    assert "catalog" in resources
    assert "schema" in resources
    assert "volume" in resources

    switch_config = SwitchConfig(install_state)
    retrieved_resources = switch_config.get_resources()

    assert retrieved_resources["catalog"] == "test_catalog"
    assert retrieved_resources["schema"] == "test_schema"
    assert retrieved_resources["volume"] == "test_volume"


def _verify_job_id_retrieval(install_state: InstallState, expected_job_id: int):
    """Verify job ID retrieval."""
    switch_config = SwitchConfig(install_state)
    job_id_from_config = switch_config.get_job_id()

    assert isinstance(job_id_from_config, int)
    assert job_id_from_config > 0
    assert job_id_from_config == expected_job_id


def _cleanup_switch(switch_deployment: SwitchDeployment, installation: Installation) -> None:
    """Cleanup Switch deployment and installation state."""
    try:
        switch_deployment.uninstall()
        installation.remove()
    except NotFound:
        pass
