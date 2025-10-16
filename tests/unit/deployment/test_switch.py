from collections.abc import Sequence
from unittest.mock import Mock, create_autospec

import pytest

from databricks.labs.blueprint.installation import Installation
from databricks.labs.blueprint.installer import InstallState
from databricks.labs.blueprint.wheels import ProductInfo
from databricks.labs.lakebridge.deployment.job import JobDeployment
from databricks.labs.lakebridge.deployment.switch import SwitchDeployment
from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import NotFound
from databricks.sdk.service.jobs import JobParameterDefinition


class FriendOfSwitchDeployment(SwitchDeployment):
    """A friend class to access protected members for testing purposes."""

    def get_switch_job_parameters(self) -> Sequence[JobParameterDefinition]:
        return self._get_switch_job_parameters()


@pytest.fixture()
def workspace_client() -> WorkspaceClient:
    ws = create_autospec(WorkspaceClient)
    ws.jobs = Mock()
    ws.jobs.delete = Mock()
    ws.jobs.get = Mock()
    ws.jobs.reset = Mock()
    ws.jobs.create = Mock()
    return ws


@pytest.fixture()
def install_state() -> InstallState:
    state = create_autospec(InstallState)
    state.jobs = {}
    state.switch_resources = {}
    return state


@pytest.fixture()
def switch_deployment(workspace_client: WorkspaceClient, install_state: InstallState) -> SwitchDeployment:
    installation = create_autospec(Installation)
    product_info = create_autospec(ProductInfo)
    job_deployer = create_autospec(JobDeployment)

    return SwitchDeployment(workspace_client, installation, install_state, product_info, job_deployer)


def test_uninstall_removes_job_and_saves_state(
    switch_deployment: SwitchDeployment, install_state, workspace_client
) -> None:
    install_state.jobs = {"Switch": "123"}
    install_state.save.reset_mock()

    workspace_client.jobs.delete.reset_mock()

    switch_deployment.uninstall()

    assert "Switch" not in install_state.jobs
    workspace_client.jobs.delete.assert_called_once_with(123)
    install_state.save.assert_called_once()


def test_uninstall_handles_missing_job(switch_deployment: SwitchDeployment, install_state, workspace_client) -> None:
    install_state.jobs = {"Switch": "123"}
    workspace_client.jobs.delete.side_effect = NotFound("missing")

    switch_deployment.uninstall()

    install_state.save.assert_called_once()


def test_get_switch_job_parameters_excludes_wait_for_completion() -> None:
    ws = create_autospec(WorkspaceClient)
    installation = create_autospec(Installation)
    state = create_autospec(InstallState)
    state.jobs = {}
    product_info = create_autospec(ProductInfo)
    job_deployer = create_autospec(JobDeployment)

    deployment = FriendOfSwitchDeployment(ws, installation, state, product_info, job_deployer)

    job_params = deployment.get_switch_job_parameters()
    param_names = {param.name for param in job_params}

    assert "input_dir" in param_names
    assert "output_dir" in param_names
    assert "result_catalog" in param_names
    assert "result_schema" in param_names
    assert "builtin_prompt" in param_names
