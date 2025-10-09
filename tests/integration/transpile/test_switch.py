from pathlib import Path

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
from databricks.labs.lakebridge.transpiler.switch_runner import SwitchConfig


def test_switch_installation(ws: WorkspaceClient, switch_artifact: Path):
    """Test Switch installation, job creation, resource persistence, and cleanup."""
    context = ApplicationContext(ws)
    installation = context.installation
    install_state = InstallState.from_installation(installation)
    transpiler_repository = TranspilerRepository.user_home()

    # Phase 1: Local installation
    installer = SwitchInstaller(transpiler_repository)
    result = installer.install(switch_artifact)
    assert result, "Switch local installation failed"

    # Phase 2: Workspace deployment
    product_info = ProductInfo.from_class(SwitchDeployment)
    job_deployer = JobDeployment(ws, installation, install_state, product_info)
    switch_deployment = SwitchDeployment(
        ws, installation, install_state, product_info, job_deployer, transpiler_repository
    )

    resources = SwitchResourcesConfig(catalog="test_catalog", schema="test_schema", volume="test_volume")
    switch_deployment.install(resources)

    try:
        install_state = InstallState.from_installation(installation)
        job_id = _verify_job_creation(ws, install_state)
        _verify_resource_persistence(install_state)
        _verify_job_id_retrieval(install_state, job_id)
    finally:
        try:
            switch_deployment.uninstall()
            installation.remove()
        except NotFound:
            pass


def _verify_job_creation(ws: WorkspaceClient, install_state: InstallState):
    """Verify job creation and registration."""
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
