import datetime as dt
import json
import os
import shutil
from pathlib import Path
from typing import Any
from unittest.mock import Mock, patch

import pytest

from databricks.labs.lakebridge.transpiler.installers import (
    ArtifactInstaller,
    MorpheusInstaller,
    SwitchInstaller,
)
from databricks.labs.lakebridge.transpiler.repository import TranspilerRepository
from databricks.sdk.errors import InvalidParameterValue, NotFound
from databricks.sdk.service.jobs import JobSettings


def test_store_product_state(tmp_path) -> None:
    """Verify the product state is stored after installing is correct."""

    class MockArtifactInstaller(ArtifactInstaller):
        @classmethod
        def store_product_state(cls, product_path: Path, version: str) -> None:
            cls._store_product_state(product_path, version)

    # Store the product state, capturing the time before and after so we can verify the timestamp it puts in there.
    before = dt.datetime.now(tz=dt.timezone.utc)
    MockArtifactInstaller.store_product_state(tmp_path, "1.2.3")
    after = dt.datetime.now(tz=dt.timezone.utc)

    # Load the state that was just stored.
    with (tmp_path / "state" / "version.json").open("r", encoding="utf-8") as f:
        stored_state = json.load(f)

    # Verify the timestamp first.
    stored_date = stored_state["date"]
    parsed_date = dt.datetime.fromisoformat(stored_date)
    assert parsed_date.tzinfo is not None, "Stored date should be timezone-aware."
    assert before <= parsed_date <= after, f"Stored date {stored_date} is not within the expected range."

    # Verify the rest, now that we've checked the timestamp.
    expected_state = {
        "version": "v1.2.3",
        "date": stored_date,
    }
    assert stored_state == expected_state


@pytest.fixture
def no_java(monkeypatch: pytest.MonkeyPatch) -> None:
    """Ensure that (temporarily) no 'java' binary can be found in the environment."""
    found_java = shutil.which("java")
    while found_java is not None:
        # Java is installed, so we need to figure out how to remove it from the path.
        # (We loop here to handle cases where multiple java binaries are available via the PATH.)
        java_directory = Path(found_java).parent
        search_path = os.environ.get("PATH", os.defpath).split(os.pathsep)
        updated_path = os.pathsep.join(p for p in search_path if p and Path(p) != java_directory)
        assert (
            search_path != updated_path
        ), f"Did not find {java_directory} in {search_path}, but 'java' was found at {found_java}."

        # Set the modified PATH without the directory where 'java' was found.
        monkeypatch.setenv("PATH", os.pathsep.join(updated_path))

        # Check again if 'java' is still found.
        found_java = shutil.which("java")


def test_java_version_with_java_missing(no_java: None) -> None:
    """Verify the Java version check handles Java missing entirely."""
    expected_missing = MorpheusInstaller.find_java()
    assert expected_missing is None


class FriendOfMorpheusInstaller(MorpheusInstaller):
    """A friend class to access protected methods for testing purposes."""

    @classmethod
    def parse_java_version(cls, output: str) -> tuple[int, int, int, int] | None:
        return cls._parse_java_version(output)


@pytest.mark.parametrize(
    ("version", "lts_suffix", "expected"),
    (
        # Real examples.
        pytest.param("1.8.0_452", False, None, id="1.8.0_452"),
        pytest.param("11.0.27", False, (11, 0, 27, 0), id="11.0.27"),
        pytest.param("17.0.15", False, (17, 0, 15, 0), id="17.0.15"),
        pytest.param("21.0.7", False, (21, 0, 7, 0), id="21.0.7"),
        pytest.param("24.0.1", False, (24, 0, 1, 0), id="24.0.1"),
        pytest.param("25", True, (25, 0, 0, 0), id="25"),
        # All digits.
        pytest.param("1.2.3.4", False, (1, 2, 3, 4), id="1.2.3.4"),
        # Trailing zeros can be omitted.
        pytest.param("1.2.3", False, (1, 2, 3, 0), id="1.2.3"),
        pytest.param("1.2", False, (1, 2, 0, 0), id="1.2"),
        pytest.param("1", False, (1, 0, 0, 0), id="1"),
        # Another edge case.
        pytest.param("", False, None, id="empty string"),
    ),
)
def test_java_version_parse(version: str, lts_suffix: bool, expected: tuple[int, int, int, int] | None) -> None:
    """Verify that the Java version parsing works correctly."""
    # Format reference: https://docs.oracle.com/en/java/javase/11/install/version-string-format.html
    # The version string for these releases looks the same, except that as of 25 the "LTS" suffix can be added.
    suffix = " LTS" if lts_suffix else ""
    version_output = f'openjdk version "{version}" 2025-06-19{suffix}'
    parsed = FriendOfMorpheusInstaller.parse_java_version(version_output)
    assert parsed == expected


def test_java_version_parse_missing() -> None:
    """Verify that we return None when the version is missing."""
    version_output = "Nothing in here that looks like a version."
    parsed = FriendOfMorpheusInstaller.parse_java_version(version_output)
    assert parsed is None


class FriendOfSwitchInstaller(SwitchInstaller):
    """A friend class to access protected methods for testing purposes."""

    def get_existing_job_id(self, install_state: Any) -> str | None:
        return self._get_existing_job_id(install_state)

    def create_or_update_switch_job(self, job_id: str | None) -> str:
        return self._create_or_update_switch_job(job_id)

    def get_switch_job_parameters(self) -> list:
        return self._get_switch_job_parameters()

    def prompt_for_switch_resources(self) -> tuple[str, str, str]:
        return self._prompt_for_switch_resources()


class TestSwitchInstaller:
    """Test suite for SwitchInstaller."""

    @pytest.fixture
    def installer(self, tmp_path: Path) -> SwitchInstaller:
        """Create a SwitchInstaller instance for testing."""
        mock_ws = Mock()
        mock_installation = Mock()
        repository = TranspilerRepository(tmp_path)
        return SwitchInstaller(repository, mock_ws, mock_installation)

    def test_name(self, installer: SwitchInstaller) -> None:
        """Verify the installer name is correct."""
        assert installer.name == "Switch"

    @pytest.mark.parametrize(
        ("filename", "expected"),
        (
            # Valid Switch wheel files
            pytest.param("databricks_switch_plugin-0.1.0-py3-none-any.whl", True, id="valid_version"),
            pytest.param("databricks_switch_plugin-1.2.3-py3-none-any.whl", True, id="valid_multi_digit"),
            pytest.param("databricks_switch_plugin-0.1.0rc1-py3-none-any.whl", True, id="valid_rc_version"),
            # Invalid files
            pytest.param("databricks_bb_plugin-0.1.0-py3-none-any.whl", False, id="wrong_package"),
            pytest.param("some_other_package-0.1.0-py3-none-any.whl", False, id="other_package"),
            pytest.param("databricks_switch_plugin-0.1.0.jar", False, id="wrong_extension"),
        ),
    )
    def test_can_install(self, filename: str, expected: bool, installer: SwitchInstaller) -> None:
        """Verify can_install works for valid and invalid files."""
        assert installer.can_install(Path(filename)) == expected

    @patch.object(SwitchInstaller, "_configure_resources")
    @patch.object(SwitchInstaller, "_setup_job")
    @patch.object(SwitchInstaller, "_deploy_workspace")
    @patch.object(SwitchInstaller, "_get_switch_package_path")
    def test_install_orchestrates_phases_correctly(
        self,
        mock_get_package_path: Mock,
        mock_deploy: Mock,
        mock_setup_job: Mock,
        mock_configure: Mock,
        installer: SwitchInstaller,
        tmp_path: Path,
    ) -> None:
        """Test install() orchestrates all phases in correct order."""
        # Setup: Package exists in site-packages
        switch_package_path = tmp_path / "databricks"
        switch_package_path.mkdir(parents=True)
        mock_get_package_path.return_value = switch_package_path

        # Act
        result = installer.install()

        # Assert: All phases called in correct order
        assert result is True
        mock_deploy.assert_called_once_with(switch_package_path)
        mock_setup_job.assert_called_once()
        mock_configure.assert_called_once()

    @patch.object(SwitchInstaller, "_get_switch_package_path")
    def test_install_returns_false_when_package_missing(
        self,
        mock_get_package_path: Mock,
        installer: SwitchInstaller,
        tmp_path: Path,
    ) -> None:
        """Test that install() returns False when Switch package not found."""
        # Setup: Package path doesn't exist
        switch_package_path = tmp_path / "databricks"
        mock_get_package_path.return_value = switch_package_path

        # Act
        result = installer.install()

        # Assert
        assert result is False

    @patch("databricks.labs.lakebridge.transpiler.installers.WheelInstaller")
    @patch.object(SwitchInstaller, "_get_switch_package_path")
    @patch.object(SwitchInstaller, "_deploy_workspace")
    @patch.object(SwitchInstaller, "_setup_job")
    @patch.object(SwitchInstaller, "_configure_resources")
    def test_install_with_existing_local_installation(
        self,
        mock_configure: Mock,
        mock_setup_job: Mock,
        mock_deploy: Mock,
        mock_get_package_path: Mock,
        mock_wheel_installer_class: Mock,
        installer: SwitchInstaller,
        tmp_path: Path,
    ) -> None:
        """Test install proceeds with deployment when local installation exists."""
        # Setup: Wheel install returns None (already installed), but package exists in site-packages
        mock_wheel_installer = Mock()
        mock_wheel_installer.install.return_value = None
        mock_wheel_installer_class.return_value = mock_wheel_installer

        # Create site-packages path
        switch_package_path = tmp_path / "databricks"
        switch_package_path.mkdir(parents=True)
        mock_get_package_path.return_value = switch_package_path

        # Act
        result = installer.install()

        # Assert: Should proceed with deployment phases
        assert result is True
        mock_deploy.assert_called_once_with(switch_package_path)
        mock_setup_job.assert_called_once()
        mock_configure.assert_called_once()

    @pytest.mark.parametrize(
        ("jobs_state", "delete_side_effect", "expect_delete_call", "expect_save"),
        (
            pytest.param({"Switch": "12345"}, None, 12345, True, id="existing_job_success"),
            pytest.param({}, None, None, False, id="no_job_in_state"),
            pytest.param({"Switch": "99999"}, NotFound("Job not found"), 99999, True, id="job_cleanup_on_error"),
        ),
    )
    @patch("databricks.labs.lakebridge.transpiler.installers.InstallState")
    def test_uninstall(
        self,
        mock_install_state_class: Mock,
        jobs_state: dict,
        delete_side_effect: Exception | None,
        expect_delete_call: int | None,
        expect_save: bool,
        installer: SwitchInstaller,
    ) -> None:
        """Test uninstall with various scenarios."""
        # Setup
        mock_install_state = Mock()
        mock_install_state.jobs = dict(jobs_state)
        mock_install_state.save = Mock()
        mock_install_state_class.from_installation.return_value = mock_install_state

        mock_ws = Mock()
        mock_ws.jobs = Mock()
        if delete_side_effect:
            mock_ws.jobs.delete = Mock(side_effect=delete_side_effect)
        else:
            mock_ws.jobs.delete = Mock()

        # Act
        with patch.object(installer, "_workspace_client", mock_ws):
            installer.uninstall()

        # Assert
        if expect_delete_call is not None:
            mock_ws.jobs.delete.assert_called_once_with(expect_delete_call)
        else:
            mock_ws.jobs.delete.assert_not_called()

        assert "Switch" not in mock_install_state.jobs

        if expect_save:
            mock_install_state.save.assert_called_once()
        else:
            mock_install_state.save.assert_not_called()

    def test_uninstall_handles_non_numeric_job_id(self, installer: SwitchInstaller) -> None:
        """Test that uninstall handles non-numeric job IDs gracefully."""
        mock_install_state = Mock()
        mock_install_state.jobs = {"Switch": "not-a-number"}
        mock_install_state.save = Mock()

        with patch(
            "databricks.labs.lakebridge.transpiler.installers.InstallState.from_installation",
            return_value=mock_install_state,
        ):
            with patch.object(installer, "_workspace_client") as mock_ws:
                with pytest.raises(ValueError):
                    installer.uninstall()

                mock_ws.jobs.delete.assert_not_called()

    @pytest.mark.parametrize(
        ("switch_resources", "expected_result"),
        (
            pytest.param(
                {"catalog": "test_catalog", "schema": "test_schema", "volume": "test_volume"},
                {"catalog": "test_catalog", "schema": "test_schema", "volume": "test_volume"},
                id="resources_configured",
            ),
            pytest.param({}, None, id="resources_not_configured"),
        ),
    )
    def test_get_configured_resources(
        self,
        switch_resources: dict,
        expected_result: dict[str, str] | None,
        installer: SwitchInstaller,
    ) -> None:
        """Test get_configured_resources returns resources when configured."""
        mock_install_state = Mock()
        mock_install_state.switch_resources = switch_resources

        with patch(
            "databricks.labs.lakebridge.transpiler.installers.InstallState.from_installation",
            return_value=mock_install_state,
        ):
            result = installer.get_configured_resources()
            assert result == expected_result

    @pytest.mark.parametrize(
        ("jobs_state", "get_side_effect", "expected"),
        (
            pytest.param({}, None, None, id="no_job_in_state"),
            pytest.param({"Switch": "12345"}, None, "12345", id="valid_job_exists"),
            pytest.param({"Switch": "99999"}, NotFound("Job not found"), None, id="job_not_found"),
            pytest.param({"Switch": "invalid"}, ValueError("Invalid job ID"), None, id="invalid_job_id"),
        ),
    )
    def test_get_existing_job_id(
        self,
        jobs_state: dict,
        get_side_effect: Exception | None,
        expected: str | None,
        tmp_path: Path,
    ) -> None:
        """Test _get_existing_job_id returns job_id if valid, None otherwise."""
        install_state = Mock()
        install_state.jobs = jobs_state

        mock_ws = Mock()
        if get_side_effect:
            mock_ws.jobs.get.side_effect = get_side_effect

        # Use friend class to access protected method
        repository = TranspilerRepository(tmp_path)
        friend_installer = FriendOfSwitchInstaller(repository, mock_ws, Mock())
        result = friend_installer.get_existing_job_id(install_state)

        assert result == expected
        if jobs_state and "Switch" in jobs_state and not get_side_effect:
            mock_ws.jobs.get.assert_called_once_with(int(jobs_state["Switch"]))

    @pytest.mark.parametrize(
        ("initial_job_id", "reset_side_effect", "expected_job_id", "expect_create", "expect_reset"),
        (
            pytest.param(None, None, "12345", True, False, id="new_job_creation"),
            pytest.param("67890", None, "67890", False, True, id="existing_job_update"),
            pytest.param(
                "99999",
                InvalidParameterValue("Job not found"),
                "12345",
                True,
                True,
                id="invalid_job_fallback_to_new",
            ),
        ),
    )
    @patch.object(SwitchInstaller, "_get_switch_job_settings")
    def test_job_creation(
        self,
        mock_get_settings: Mock,
        initial_job_id: str | None,
        reset_side_effect: Exception | None,
        expected_job_id: str,
        expect_create: bool,
        expect_reset: bool,
        tmp_path: Path,
    ) -> None:
        """Test Switch job creation and update scenarios."""
        # Setup
        mock_get_settings.return_value = {"name": "test_job"}

        mock_job = Mock()
        mock_job.job_id = 12345

        mock_ws = Mock()
        mock_jobs = Mock()
        mock_ws.jobs = mock_jobs
        mock_jobs.create.return_value = mock_job

        if reset_side_effect:
            mock_jobs.reset.side_effect = reset_side_effect
        else:
            mock_jobs.reset.return_value = None

        # Act
        test_repository = TranspilerRepository(tmp_path)
        mock_installation = Mock()
        friend_installer = FriendOfSwitchInstaller(test_repository, mock_ws, mock_installation)
        result = friend_installer.create_or_update_switch_job(initial_job_id)

        # Assert
        assert result == expected_job_id

        if expect_reset:
            if reset_side_effect:
                assert initial_job_id is not None
                mock_jobs.reset.assert_called_once_with(int(initial_job_id), JobSettings(name="test_job"))
            else:
                mock_jobs.reset.assert_called_once_with(int(expected_job_id), JobSettings(name="test_job"))
        else:
            mock_jobs.reset.assert_not_called()

        if expect_create:
            mock_jobs.create.assert_called_once_with(name="test_job")
        else:
            mock_jobs.create.assert_not_called()

    def test_get_switch_job_parameters_handles_various_default_values(
        self, installer: SwitchInstaller, tmp_path: Path
    ) -> None:
        """Test that different default values in config are correctly converted."""
        mock_config = Mock()
        mock_config.options = {
            "all": [
                Mock(flag="flag1", default="<none>"),  # Should convert to ""
                Mock(flag="flag2", default=123),  # Should convert to "123"
                Mock(flag="flag3", default=None),  # Should convert to ""
                Mock(flag="flag4", default="value"),  # Should remain "value"
                Mock(flag="flag5", default=3.14),  # Should convert to "3.14"
            ]
        }

        test_repository = TranspilerRepository(tmp_path)
        mock_ws = Mock()
        mock_installation = Mock()
        friend_installer = FriendOfSwitchInstaller(test_repository, mock_ws, mock_installation)

        with patch.object(test_repository, "all_transpiler_configs", return_value={"switch": mock_config}):
            params = friend_installer.get_switch_job_parameters()

        # Convert list to dict for easier testing
        params_dict = {p.name: p.default for p in params}

        assert "input_dir" in params_dict
        assert "output_dir" in params_dict
        assert "result_catalog" in params_dict
        assert "result_schema" in params_dict
        assert "builtin_prompt" in params_dict
        assert params_dict["flag1"] == ""
        assert params_dict["flag2"] == "123"
        assert params_dict["flag3"] == ""
        assert params_dict["flag4"] == "value"
        assert params_dict["flag5"] == "3.14"

    def test_get_switch_job_parameters_raises_when_config_missing(
        self, installer: SwitchInstaller, tmp_path: Path
    ) -> None:
        """Test that ValueError is raised when Switch config is not found."""
        test_repository = TranspilerRepository(tmp_path)
        mock_ws = Mock()
        mock_installation = Mock()
        friend_installer = FriendOfSwitchInstaller(test_repository, mock_ws, mock_installation)

        with patch.object(test_repository, "all_transpiler_configs", return_value={}):
            with pytest.raises(ValueError, match="Switch config.yml not found"):
                friend_installer.get_switch_job_parameters()

    @patch("databricks.labs.lakebridge.transpiler.installers.ResourceConfigurator")
    @patch("databricks.labs.lakebridge.transpiler.installers.CatalogOperations")
    @patch("databricks.labs.lakebridge.transpiler.installers.Prompts")
    def test_prompt_for_switch_resources_success(
        self,
        mock_prompts_class: Mock,
        mock_catalog_ops_class: Mock,
        mock_configurator_class: Mock,
        tmp_path: Path,
    ) -> None:
        """Test that _prompt_for_switch_resources successfully prompts and returns resources."""
        # Setup
        mock_prompts = Mock()
        mock_prompts_class.return_value = mock_prompts

        mock_catalog_ops = Mock()
        mock_catalog_ops_class.return_value = mock_catalog_ops

        mock_configurator = Mock()
        mock_configurator_class.return_value = mock_configurator
        mock_configurator.prompt_for_catalog_setup.return_value = "my_catalog"
        mock_configurator.prompt_for_schema_setup.return_value = "my_schema"
        mock_configurator.prompt_for_volume_setup.return_value = "my_volume"

        # Create friend installer to test protected method
        mock_ws = Mock()
        mock_installation = Mock()
        repository = TranspilerRepository(tmp_path)
        friend_installer = FriendOfSwitchInstaller(repository, mock_ws, mock_installation)

        catalog, schema, volume = friend_installer.prompt_for_switch_resources()

        assert catalog == "my_catalog"
        assert schema == "my_schema"
        assert volume == "my_volume"
        mock_prompts_class.assert_called_once()
        mock_catalog_ops_class.assert_called_once_with(mock_ws)
        mock_configurator_class.assert_called_once_with(mock_ws, mock_prompts, mock_catalog_ops)
        mock_configurator.prompt_for_catalog_setup.assert_called_once()
        mock_configurator.prompt_for_schema_setup.assert_called_once_with("my_catalog", "switch")
        mock_configurator.prompt_for_volume_setup.assert_called_once_with("my_catalog", "my_schema", "switch_volume")

    @patch("databricks.labs.lakebridge.transpiler.installers.ResourceConfigurator")
    @patch("databricks.labs.lakebridge.transpiler.installers.CatalogOperations")
    @patch("databricks.labs.lakebridge.transpiler.installers.Prompts")
    def test_prompt_for_switch_resources_abort(
        self,
        mock_prompts_class: Mock,
        mock_catalog_ops_class: Mock,
        mock_configurator_class: Mock,
        tmp_path: Path,
    ) -> None:
        """Test that _prompt_for_switch_resources raises SystemExit when user aborts."""
        # Setup
        mock_prompts = Mock()
        mock_prompts_class.return_value = mock_prompts

        mock_catalog_ops = Mock()
        mock_catalog_ops_class.return_value = mock_catalog_ops

        mock_configurator = Mock()
        mock_configurator_class.return_value = mock_configurator
        mock_configurator.prompt_for_catalog_setup.side_effect = SystemExit("User aborted")

        # Create friend installer to test protected method
        mock_ws = Mock()
        mock_installation = Mock()
        repository = TranspilerRepository(tmp_path)
        friend_installer = FriendOfSwitchInstaller(repository, mock_ws, mock_installation)

        with pytest.raises(SystemExit):
            friend_installer.prompt_for_switch_resources()
