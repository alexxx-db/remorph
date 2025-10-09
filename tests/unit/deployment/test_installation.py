import logging
from pathlib import Path
from unittest.mock import create_autospec

import pytest
from databricks.labs.blueprint.installation import MockInstallation, Installation
from databricks.labs.blueprint.tui import MockPrompts
from databricks.labs.blueprint.wheels import WheelsV2, ProductInfo
from databricks.labs.blueprint.upgrades import Upgrades

from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import NotFound
from databricks.sdk.service import iam

from databricks.labs.lakebridge.config import (
    TranspileConfig,
    LakebridgeConfiguration,
    ReconcileConfig,
    DatabaseConfig,
    ReconcileMetadataConfig,
    SwitchResourcesConfig,
)
from databricks.labs.lakebridge.deployment.installation import WorkspaceInstallation
from databricks.labs.lakebridge.deployment.recon import ReconDeployment
from databricks.labs.lakebridge.deployment.switch import SwitchDeployment
from databricks.labs.lakebridge.transpiler.repository import TranspilerRepository


@pytest.fixture
def ws():
    w = create_autospec(WorkspaceClient)
    w.current_user.me.side_effect = lambda: iam.User(
        user_name="me@example.com", groups=[iam.ComplexValue(display="admins")]
    )
    return w


def test_install_all(ws):
    prompts = MockPrompts(
        {
            r"Enter catalog name": "remorph",
        }
    )
    recon_deployment = create_autospec(ReconDeployment)
    switch_deployment = create_autospec(SwitchDeployment)
    installation = create_autospec(Installation)
    product_info = create_autospec(ProductInfo)
    upgrades = create_autospec(Upgrades)

    transpile_config = TranspileConfig(
        transpiler_config_path="sqlglot",
        source_dialect="snowflake",
        input_source="/tmp/queries/snow6",
        output_folder="/tmp/queries/databricks6",
        skip_validation=True,
        catalog_name="remorph6",
        schema_name="transpiler6",
    )
    reconcile_config = ReconcileConfig(
        data_source="oracle",
        report_type="all",
        secret_scope="remorph_oracle6",
        database_config=DatabaseConfig(
            source_schema="tpch_sf10006",
            target_catalog="tpch6",
            target_schema="1000gb6",
        ),
        metadata_config=ReconcileMetadataConfig(
            catalog="remorph6",
            schema="reconcile6",
            volume="reconcile_volume6",
        ),
    )
    config = LakebridgeConfiguration(transpile=transpile_config, reconcile=reconcile_config)
    installation = WorkspaceInstallation(
        ws, prompts, installation, recon_deployment, switch_deployment, product_info, upgrades
    )
    installation.install(config)


def test_no_recon_component_installation(ws):
    prompts = MockPrompts({})
    recon_deployment = create_autospec(ReconDeployment)
    switch_deployment = create_autospec(SwitchDeployment)
    installation = create_autospec(Installation)
    product_info = create_autospec(ProductInfo)
    upgrades = create_autospec(Upgrades)

    transpile_config = TranspileConfig(
        transpiler_config_path="sqlglot",
        source_dialect="snowflake",
        input_source="/tmp/queries/snow7",
        output_folder="/tmp/queries/databricks7",
        skip_validation=True,
        catalog_name="remorph7",
        schema_name="transpiler7",
    )
    config = LakebridgeConfiguration(transpile=transpile_config)
    installation = WorkspaceInstallation(
        ws, prompts, installation, recon_deployment, switch_deployment, product_info, upgrades
    )
    installation.install(config)
    recon_deployment.install.assert_not_called()


def test_recon_component_installation(ws):
    recon_deployment = create_autospec(ReconDeployment)
    switch_deployment = create_autospec(SwitchDeployment)
    installation = create_autospec(Installation)
    prompts = MockPrompts({})
    product_info = create_autospec(ProductInfo)
    upgrades = create_autospec(Upgrades)

    reconcile_config = ReconcileConfig(
        data_source="oracle",
        report_type="all",
        secret_scope="remorph_oracle8",
        database_config=DatabaseConfig(
            source_schema="tpch_sf10008",
            target_catalog="tpch8",
            target_schema="1000gb8",
        ),
        metadata_config=ReconcileMetadataConfig(
            catalog="remorph8",
            schema="reconcile8",
            volume="reconcile_volume8",
        ),
    )
    config = LakebridgeConfiguration(reconcile=reconcile_config)
    installation = WorkspaceInstallation(
        ws, prompts, installation, recon_deployment, switch_deployment, product_info, upgrades
    )
    installation.install(config)
    recon_deployment.install.assert_called()


def test_negative_uninstall_confirmation(ws):
    prompts = MockPrompts(
        {
            r"Do you want to uninstall Lakebridge .*": "no",
        }
    )
    installation = create_autospec(Installation)
    recon_deployment = create_autospec(ReconDeployment)
    switch_deployment = create_autospec(SwitchDeployment)
    wheels = create_autospec(WheelsV2)
    upgrades = create_autospec(Upgrades)

    ws_installation = WorkspaceInstallation(
        ws, prompts, installation, recon_deployment, switch_deployment, wheels, upgrades
    )
    config = LakebridgeConfiguration()
    ws_installation.uninstall(config)
    installation.remove.assert_not_called()


def test_missing_installation(ws):
    prompts = MockPrompts(
        {
            r"Do you want to uninstall Lakebridge .*": "yes",
        }
    )
    installation = create_autospec(Installation)
    installation.files.side_effect = NotFound("Installation not found")
    installation.install_folder.return_value = "~/mock"
    recon_deployment = create_autospec(ReconDeployment)
    switch_deployment = create_autospec(SwitchDeployment)
    wheels = create_autospec(WheelsV2)
    upgrades = create_autospec(Upgrades)

    ws_installation = WorkspaceInstallation(
        ws, prompts, installation, recon_deployment, switch_deployment, wheels, upgrades
    )
    config = LakebridgeConfiguration()
    ws_installation.uninstall(config)
    installation.remove.assert_not_called()


def test_uninstall_configs_exist(ws):
    prompts = MockPrompts(
        {
            r"Do you want to uninstall Lakebridge .*": "yes",
        }
    )

    transpile_config = TranspileConfig(
        transpiler_config_path="sqlglot",
        source_dialect="snowflake",
        input_source="sf_queries1",
        output_folder="out_dir1",
        skip_validation=True,
        catalog_name="transpiler_test1",
        schema_name="convertor_test1",
        sdk_config={"warehouse_id": "abc"},
    )

    reconcile_config = ReconcileConfig(
        data_source="snowflake",
        report_type="all",
        secret_scope="remorph_snowflake1",
        database_config=DatabaseConfig(
            source_catalog="snowflake_sample_data1",
            source_schema="tpch_sf10001",
            target_catalog="tpch1",
            target_schema="1000gb1",
        ),
        metadata_config=ReconcileMetadataConfig(
            catalog="remorph1",
            schema="reconcile1",
            volume="reconcile_volume1",
        ),
    )
    config = LakebridgeConfiguration(transpile=transpile_config, reconcile=reconcile_config)
    installation = MockInstallation({})
    recon_deployment = create_autospec(ReconDeployment)
    switch_deployment = create_autospec(SwitchDeployment)
    wheels = create_autospec(WheelsV2)
    upgrades = create_autospec(Upgrades)

    ws_installation = WorkspaceInstallation(
        ws, prompts, installation, recon_deployment, switch_deployment, wheels, upgrades
    )
    ws_installation.uninstall(config)
    recon_deployment.uninstall.assert_called()
    installation.assert_removed()


def test_uninstall_configs_missing(ws):
    prompts = MockPrompts(
        {
            r"Do you want to uninstall Lakebridge .*": "yes",
        }
    )
    installation = MockInstallation()
    recon_deployment = create_autospec(ReconDeployment)
    switch_deployment = create_autospec(SwitchDeployment)
    wheels = create_autospec(WheelsV2)
    upgrades = create_autospec(Upgrades)

    ws_installation = WorkspaceInstallation(
        ws, prompts, installation, recon_deployment, switch_deployment, wheels, upgrades
    )
    config = LakebridgeConfiguration()
    ws_installation.uninstall(config)
    recon_deployment.uninstall.assert_not_called()
    installation.assert_removed()


class TestSwitchInstallation:
    """Tests for Switch transpiler installation."""

    class _StubTranspilerRepository:
        def __init__(self, names: set[str], base_path: Path):
            self._names = names
            self._base_path = base_path

        def all_transpiler_names(self) -> set[str]:
            return self._names

        def transpilers_path(self) -> Path:
            return self._base_path

    @pytest.fixture
    def switch_repository(self, monkeypatch, tmp_path) -> "_StubTranspilerRepository":
        repository = self._StubTranspilerRepository({"switch"}, tmp_path / "repository")

        def _user_home(_: type[TranspilerRepository]) -> "TestSwitchInstallation._StubTranspilerRepository":
            return repository

        monkeypatch.setattr(TranspilerRepository, "user_home", classmethod(_user_home))
        return repository

    def test_switch_install_uses_configured_resources(self, ws, switch_repository):
        prompts = MockPrompts({})
        recon_deployment = create_autospec(ReconDeployment)
        switch_deployment = create_autospec(SwitchDeployment)
        installation = create_autospec(Installation)
        product_info = create_autospec(ProductInfo)
        upgrades = create_autospec(Upgrades)

        switch_resources = SwitchResourcesConfig(catalog="cat", schema="sch", volume="vol")
        transpile_config = TranspileConfig(
            include_llm=True,
            switch_resources=switch_resources,
        )
        config = LakebridgeConfiguration(transpile=transpile_config)

        ws_installation = WorkspaceInstallation(
            ws, prompts, installation, recon_deployment, switch_deployment, product_info, upgrades
        )

        ws_installation.install(config)

        switch_deployment.install.assert_called_once()
        args, _kwargs = switch_deployment.install.call_args
        assert isinstance(args[0], Path)
        assert args[1] is switch_resources

    def test_switch_install_missing_resources_logs_error(self, ws, switch_repository, caplog):
        prompts = MockPrompts({})
        recon_deployment = create_autospec(ReconDeployment)
        switch_deployment = create_autospec(SwitchDeployment)
        installation = create_autospec(Installation)
        product_info = create_autospec(ProductInfo)
        upgrades = create_autospec(Upgrades)

        transpile_config = TranspileConfig(include_llm=True)
        config = LakebridgeConfiguration(transpile=transpile_config)

        ws_installation = WorkspaceInstallation(
            ws, prompts, installation, recon_deployment, switch_deployment, product_info, upgrades
        )

        with caplog.at_level(logging.ERROR):
            ws_installation.install(config)

        switch_deployment.install.assert_not_called()
        assert any(
            "Switch resources are missing" in record.message
            for record in caplog.records
            if record.levelno == logging.ERROR
        )
