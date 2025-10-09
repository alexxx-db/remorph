from unittest.mock import Mock, create_autospec

import pytest

from databricks.labs.blueprint.installation import Installation
from databricks.labs.blueprint.installer import InstallState
from databricks.labs.blueprint.wheels import ProductInfo
from databricks.labs.lakebridge.config import SwitchResourcesConfig
from databricks.labs.lakebridge.deployment.job import JobDeployment
from databricks.labs.lakebridge.deployment.switch import SwitchDeployment
from databricks.labs.lakebridge.transpiler.repository import TranspilerRepository
from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import NotFound


@pytest.fixture()
def workspace_client():
    ws = create_autospec(WorkspaceClient)
    ws.jobs = Mock()
    ws.jobs.delete = Mock()
    ws.jobs.get = Mock()
    ws.jobs.reset = Mock()
    ws.jobs.create = Mock()
    return ws


@pytest.fixture()
def install_state():
    state = create_autospec(InstallState)
    state.jobs = {}
    state.switch_resources = {}
    return state


@pytest.fixture()
def switch_deployment(workspace_client, install_state):
    installation = create_autospec(Installation)
    product_info = create_autospec(ProductInfo)
    job_deployer = create_autospec(JobDeployment)
    repository = create_autospec(TranspilerRepository)

    return SwitchDeployment(  # type: ignore[call-arg]
        workspace_client, installation, install_state, product_info, job_deployer, repository
    )


def test_record_resources_persists_install_state(switch_deployment, install_state, monkeypatch, tmp_path):
    resources = SwitchResourcesConfig(catalog="cat", schema="sch", volume="vol")

    install_state.switch_resources = {}
    install_state.save.reset_mock()
    monkeypatch.setattr(switch_deployment, "_deploy_workspace", lambda _: None)
    monkeypatch.setattr(switch_deployment, "_setup_job", lambda: None)

    switch_deployment.install(tmp_path, resources)

    saved = install_state.switch_resources
    assert saved["catalog"] == "cat"
    assert saved["schema"] == "sch"
    assert saved["volume"] == "vol"
    install_state.save.assert_called_once()


def test_install_records_resources(switch_deployment, monkeypatch, tmp_path):
    resources = SwitchResourcesConfig(catalog="cat", schema="sch", volume="vol")
    package = tmp_path
    call_order = []

    monkeypatch.setattr(switch_deployment, "_deploy_workspace", lambda pkg: call_order.append(("deploy", pkg)))
    monkeypatch.setattr(switch_deployment, "_setup_job", lambda: call_order.append(("setup", None)))
    monkeypatch.setattr(switch_deployment, "_record_resources", lambda res: call_order.append(("record", res)))

    switch_deployment.install(package, resources)

    assert ("deploy", package) in call_order
    assert ("setup", None) in call_order
    assert ("record", resources) in call_order


def test_uninstall_removes_job_and_saves_state(switch_deployment, install_state, workspace_client):
    install_state.jobs = {"Switch": "123"}
    install_state.save.reset_mock()

    workspace_client.jobs.delete.reset_mock()

    switch_deployment.uninstall()

    assert "Switch" not in install_state.jobs
    workspace_client.jobs.delete.assert_called_once_with(123)
    install_state.save.assert_called_once()


def test_uninstall_handles_missing_job(switch_deployment, install_state, workspace_client):
    install_state.jobs = {"Switch": "123"}
    workspace_client.jobs.delete.side_effect = NotFound("missing")

    switch_deployment.uninstall()

    install_state.save.assert_called_once()


def test_get_configured_resources_returns_mapping(switch_deployment, install_state):
    install_state.switch_resources = {"catalog": "c", "schema": "s", "volume": "v"}
    resources = switch_deployment.get_configured_resources()
    assert resources == {"catalog": "c", "schema": "s", "volume": "v"}


def test_get_configured_resources_none_when_absent(switch_deployment, install_state):
    install_state.switch_resources = {}
    assert switch_deployment.get_configured_resources() is None
