import os
import random
import string
from dataclasses import dataclass
from datetime import datetime
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
from databricks.labs.lakebridge.helpers.metastore import CatalogOperations
from databricks.labs.lakebridge.transpiler.installers import SwitchInstaller
from databricks.labs.lakebridge.transpiler.repository import TranspilerRepository
from databricks.labs.lakebridge.transpiler.switch_runner import SwitchConfig, SwitchRunner

E2E_FLAG = "LAKEBRIDGE_SWITCH_E2E"
E2E_CATALOG_ENV = "LAKEBRIDGE_SWITCH_E2E_CATALOG"
E2E_SCHEMA_ENV = "LAKEBRIDGE_SWITCH_E2E_SCHEMA"
E2E_VOLUME_ENV = "LAKEBRIDGE_SWITCH_E2E_VOLUME"
DEFAULT_E2E_CATALOG = "main"
DEFAULT_E2E_SCHEMA_PREFIX = "e2e_lakebridge_switch"
DEFAULT_E2E_VOLUME = "switch_volume"
OUTPUT_NOTEBOOK_NAME = "test_ctas_simple"
TEST_SQL_FILE = (
    Path(__file__).parent.parent.parent / "resources" / "functional" / "snowflake" / "ddl" / "test_ctas_simple.sql"
)


@dataclass(frozen=True)
class E2EResources:
    """Resolved resources used by the end-to-end transpile test."""

    catalog: str
    schema_prefix: str
    schema: str
    volume: str
    output_folder: str


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
    if os.getenv(E2E_FLAG) != "true":
        pytest.skip("E2E test requires LAKEBRIDGE_SWITCH_E2E=true")

    resources = _resolve_e2e_resources(ws)
    catalog_ops = CatalogOperations(ws)
    catalog_ops.create_schema(resources.schema, resources.catalog)
    catalog_ops.create_volume(resources.catalog, resources.schema, resources.volume)

    setup = _setup_switch(ws, switch_artifact, resources.catalog, resources.schema, resources.volume)

    try:
        run_results = _run_switch_transpile(
            ws,
            setup.installation,
            resources.output_folder,
            resources.catalog,
            resources.schema,
            resources.volume,
            setup.job_id,
        )
        _verify_transpile_results(run_results, setup.job_id)
        notebook_path = f"{resources.output_folder}/{OUTPUT_NOTEBOOK_NAME}"
        _verify_output_notebook(ws, notebook_path)
    finally:
        _cleanup_switch(setup.switch_deployment, setup.installation)
        _drop_schema(ws, resources.catalog, resources.schema)
        _cleanup_output_folder(ws, resources.output_folder)


def _generate_unique_suffix() -> str:
    """Generate unique suffix using timestamp + random string for parallel execution safety."""
    time_part = datetime.now().strftime("%Y%m%d%H%M%S")
    random_part = "".join(random.choices(string.ascii_lowercase + string.digits, k=4))
    return f"{time_part}_{random_part}"


def _resolve_e2e_resources(ws: WorkspaceClient) -> E2EResources:
    """Resolve catalog/schema/volume and output folder for the end-to-end test."""
    catalog = os.getenv(E2E_CATALOG_ENV, DEFAULT_E2E_CATALOG)
    schema_prefix = os.getenv(E2E_SCHEMA_ENV, DEFAULT_E2E_SCHEMA_PREFIX)
    volume = os.getenv(E2E_VOLUME_ENV, DEFAULT_E2E_VOLUME)
    unique_suffix = _generate_unique_suffix()
    schema = f"{schema_prefix}_{unique_suffix}"
    current_user = ws.current_user.me().user_name
    output_folder = f"/Workspace/Users/{current_user}/.{schema_prefix}_{unique_suffix}"
    return E2EResources(
        catalog=catalog,
        schema_prefix=schema_prefix,
        schema=schema,
        volume=volume,
        output_folder=output_folder,
    )


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
    runner = SwitchRunner(ws, installation)
    return runner.run(
        input_source=str(TEST_SQL_FILE),
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
    assert isinstance(run_results, list), f"Unexpected results type: {type(run_results)}"
    assert len(run_results) == 1, f"Expected single run result, got {len(run_results)}"
    result = cast(dict, run_results[0])
    assert result.get("job_id") == expected_job_id, result
    assert result.get("state") == "TERMINATED", result
    assert result.get("result_state") == "SUCCESS", result


def _verify_output_notebook(ws: WorkspaceClient, output_path: str) -> None:
    """Verify output notebook creation."""
    notebook_info = ws.workspace.get_status(output_path)
    assert notebook_info.object_type is not None, f"Notebook not found at {output_path}"
    assert notebook_info.object_type.value == "NOTEBOOK", f"Unexpected object type for {output_path}: {notebook_info}"


def _get_and_verify_job_id(ws: WorkspaceClient, install_state: InstallState):
    """Get job ID and verify job creation and registration."""
    assert "Switch" in install_state.jobs, "Switch job not persisted in install state"
    job_id = int(install_state.jobs["Switch"])

    job = ws.jobs.get(job_id)
    assert job is not None, f"Job {job_id} could not be retrieved"
    assert job.settings is not None, f"Job {job_id} missing settings"
    assert job.settings.name is not None, f"Job {job_id} missing name"
    assert "switch" in job.settings.name.lower(), f"Unexpected job name: {job.settings.name}"

    assert job.settings.tasks is not None
    assert len(job.settings.tasks) > 0
    task = job.settings.tasks[0]
    assert task.notebook_task is not None, "First task is not a notebook task"
    assert (
        "switch" in task.notebook_task.notebook_path.lower()
    ), f"Unexpected notebook path: {task.notebook_task.notebook_path}"
    return job_id


def _verify_resource_persistence(install_state: InstallState):
    """Verify resource persistence."""
    assert install_state.switch_resources is not None, "Switch resources missing from install state"
    resources = install_state.switch_resources
    for field in ("catalog", "schema", "volume"):
        assert field in resources, f"Switch resource missing '{field}' entry"

    switch_config = SwitchConfig(install_state)
    retrieved_resources = switch_config.get_resources()

    assert retrieved_resources["catalog"] == "test_catalog", retrieved_resources
    assert retrieved_resources["schema"] == "test_schema", retrieved_resources
    assert retrieved_resources["volume"] == "test_volume", retrieved_resources


def _verify_job_id_retrieval(install_state: InstallState, expected_job_id: int):
    """Verify job ID retrieval."""
    switch_config = SwitchConfig(install_state)
    job_id_from_config = switch_config.get_job_id()

    assert isinstance(job_id_from_config, int), f"Invalid job id type: {type(job_id_from_config)}"
    assert job_id_from_config > 0, f"Job id should be positive: {job_id_from_config}"
    assert job_id_from_config == expected_job_id, f"Job id mismatch: {job_id_from_config} != {expected_job_id}"


def _cleanup_output_folder(ws: WorkspaceClient, output_folder: str) -> None:
    """Remove generated notebooks from the workspace if they exist."""
    try:
        ws.workspace.delete(output_folder, recursive=True)
    except NotFound:
        pass


def _drop_schema(ws: WorkspaceClient, catalog: str, schema: str) -> None:
    """Drop the temporary schema used during the test."""
    try:
        ws.schemas.delete(f"{catalog}.{schema}", force=True)
    except NotFound:
        pass


def _cleanup_switch(switch_deployment: SwitchDeployment, installation: Installation) -> None:
    """Cleanup Switch deployment and installation state."""
    try:
        switch_deployment.uninstall()
        installation.remove()
    except NotFound:
        pass
