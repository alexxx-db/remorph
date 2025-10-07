from pathlib import Path
from unittest.mock import Mock, create_autospec

import pytest

from databricks.labs.blueprint.installer import InstallState
from databricks.labs.lakebridge.transpiler.switch_runner import SwitchConfig, SwitchRunner


class FriendOfSwitchRunner(SwitchRunner):
    """Friend class to access protected methods for testing."""

    def upload_to_volume(self, local_path: Path, catalog: str, schema: str, volume: str) -> str:
        return self._upload_to_volume(local_path, catalog, schema, volume)

    def build_job_parameters(
        self,
        volume_input_path: str,
        output_ws_folder: str,
        catalog: str,
        schema: str,
        source_dialect: str,
        switch_options: dict[str, str],
    ) -> dict[str, str]:
        return self._build_job_parameters(
            volume_input_path, output_ws_folder, catalog, schema, source_dialect, switch_options
        )


class TestSwitchConfig:
    """Test suite for SwitchConfig."""

    @pytest.fixture
    def install_state_with_switch(self) -> InstallState:
        """Create InstallState with Switch configured."""
        state = create_autospec(InstallState)
        state.switch_resources = {
            "catalog": "test_catalog",
            "schema": "test_schema",
            "volume": "test_volume",
        }
        state.jobs = {"Switch": "12345"}
        return state

    @pytest.fixture
    def install_state_without_switch(self) -> InstallState:
        """Create InstallState without Switch configured."""
        state = create_autospec(InstallState)
        state.switch_resources = None
        state.jobs = {}
        return state

    def test_get_resources_success(self, install_state_with_switch: InstallState) -> None:
        """Verify successful resource retrieval from InstallState."""
        config = SwitchConfig(install_state_with_switch)
        resources = config.get_resources()

        assert resources == {
            "catalog": "test_catalog",
            "schema": "test_schema",
            "volume": "test_volume",
        }

    @pytest.mark.parametrize(
        ("switch_resources", "error_msg_fragment"),
        (
            pytest.param(None, "Switch resources not configured", id="resources_none"),
            pytest.param({"catalog": "cat"}, "Switch resources not configured", id="missing_schema_volume"),
            pytest.param({"catalog": "cat", "schema": "sch"}, "Switch resources not configured", id="missing_volume"),
        ),
    )
    def test_get_resources_not_configured(self, switch_resources: dict | None, error_msg_fragment: str) -> None:
        """Test error when switch_resources missing or incomplete."""
        state = create_autospec(InstallState)
        state.switch_resources = switch_resources

        config = SwitchConfig(state)

        with pytest.raises(SystemExit, match=error_msg_fragment):
            config.get_resources()

    @pytest.mark.parametrize(
        ("jobs", "should_succeed"),
        (
            pytest.param({"Switch": "12345"}, True, id="job_exists"),
            pytest.param({"Switch": 67890}, True, id="job_exists_int"),
            pytest.param({}, False, id="job_missing"),
            pytest.param({"OtherJob": "99999"}, False, id="other_job_only"),
        ),
    )
    def test_get_job_id(self, jobs: dict, should_succeed: bool) -> None:
        """Test job ID retrieval from InstallState."""
        state = create_autospec(InstallState)
        state.jobs = jobs

        config = SwitchConfig(state)

        if should_succeed:
            job_id = config.get_job_id()
            assert isinstance(job_id, int)
            assert job_id == int(jobs["Switch"])
        else:
            with pytest.raises(SystemExit, match="Switch Job ID not found"):
                config.get_job_id()


class TestSwitchRunner:
    """Test suite for SwitchRunner."""

    @pytest.fixture
    def mock_ws(self) -> Mock:
        """Create mock WorkspaceClient."""
        ws = Mock()
        ws.config.host = "https://test.databricks.com"
        ws.files.upload = Mock()
        ws.jobs.run_now = Mock()
        ws.jobs.run_now_and_wait = Mock()
        return ws

    @pytest.fixture
    def mock_installation(self) -> Mock:
        """Create mock Installation."""
        return Mock()

    @pytest.fixture
    def runner(self, mock_ws: Mock, mock_installation: Mock) -> FriendOfSwitchRunner:
        """Create SwitchRunner instance."""
        return FriendOfSwitchRunner(mock_ws, mock_installation)

    @pytest.mark.parametrize(
        "is_file",
        (
            pytest.param(True, id="single_file"),
            pytest.param(False, id="directory"),
        ),
    )
    def test_upload_to_volume(self, runner: FriendOfSwitchRunner, mock_ws: Mock, tmp_path: Path, is_file: bool) -> None:
        """Test file/directory upload to Volume."""
        # Setup test files
        if is_file:
            test_file = tmp_path / "test.sql"
            test_file.write_text("SELECT 1;")
            local_path = test_file
            expected_calls = 1
        else:
            test_dir = tmp_path / "queries"
            test_dir.mkdir()
            (test_dir / "query1.sql").write_text("SELECT 1;")
            (test_dir / "query2.sql").write_text("SELECT 2;")
            subdir = test_dir / "subdir"
            subdir.mkdir()
            (subdir / "query3.sql").write_text("SELECT 3;")
            local_path = test_dir
            expected_calls = 3

        # Execute upload
        volume_path = runner.upload_to_volume(
            local_path=local_path, catalog="test_cat", schema="test_sch", volume="test_vol"
        )

        # Verify results
        assert volume_path.startswith("/Volumes/test_cat/test_sch/test_vol/input_")
        assert mock_ws.files.upload.call_count == expected_calls

    def test_upload_unique_paths(self, runner: FriendOfSwitchRunner, tmp_path: Path) -> None:
        """Verify UUID usage prevents path collisions."""
        test_file = tmp_path / "test.sql"
        test_file.write_text("SELECT 1;")

        # Generate two upload paths
        path1 = runner.upload_to_volume(test_file, "cat", "sch", "vol")
        path2 = runner.upload_to_volume(test_file, "cat", "sch", "vol")

        # Paths should be different due to UUID
        assert path1 != path2
        assert path1.startswith("/Volumes/cat/sch/vol/input_")
        assert path2.startswith("/Volumes/cat/sch/vol/input_")

    def test_upload_preserves_hierarchy(self, runner: FriendOfSwitchRunner, mock_ws: Mock, tmp_path: Path) -> None:
        """Verify subdirectory structure is maintained."""
        # Create nested directory structure
        root = tmp_path / "root"
        root.mkdir()
        (root / "file1.sql").write_text("SELECT 1;")
        subdir = root / "sub" / "deep"
        subdir.mkdir(parents=True)
        (subdir / "file2.sql").write_text("SELECT 2;")

        # Upload directory
        runner.upload_to_volume(root, "cat", "sch", "vol")

        # Verify hierarchy is preserved
        calls = [call[1]["file_path"] for call in mock_ws.files.upload.call_args_list]
        assert any("file1.sql" in path and "/sub/" not in path for path in calls)
        assert any("file2.sql" in path and "/sub/deep/" in path for path in calls)

    @pytest.mark.parametrize(
        ("switch_options", "expected_extra_keys"),
        (
            pytest.param({}, 0, id="no_options"),
            pytest.param({"endpoint_name": "claude", "concurrency": "4", "log_level": "DEBUG"}, 3, id="with_options"),
        ),
    )
    def test_build_job_parameters(
        self, runner: FriendOfSwitchRunner, switch_options: dict, expected_extra_keys: int
    ) -> None:
        """Test job parameter construction."""
        params = runner.build_job_parameters(
            volume_input_path="/Volumes/cat/sch/vol/input_123",
            output_ws_folder="/Workspace/Users/test/output",
            catalog="test_cat",
            schema="test_sch",
            source_dialect="snowflake",
            switch_options=switch_options,
        )

        # Verify required parameters
        assert params["input_dir"] == "/Volumes/cat/sch/vol/input_123"
        assert params["output_dir"] == "/Workspace/Users/test/output"
        assert params["result_catalog"] == "test_cat"
        assert params["result_schema"] == "test_sch"
        assert params["builtin_prompt"] == "snowflake"

        # Verify options are included
        assert len(params) == 5 + expected_extra_keys
        for key, value in switch_options.items():
            assert params[key] == value

    @pytest.mark.parametrize(
        "wait_for_completion",
        (
            pytest.param(False, id="async_execution"),
            pytest.param(True, id="sync_execution"),
        ),
    )
    def test_run_job_execution(
        self, runner: FriendOfSwitchRunner, mock_ws: Mock, tmp_path: Path, wait_for_completion: bool
    ) -> None:
        """Test async/sync job execution based on wait_for_completion flag."""
        # Setup test file
        test_file = tmp_path / "test.sql"
        test_file.write_text("SELECT 1;")

        # Mock job execution
        mock_run = Mock()
        mock_run.run_id = 99999
        if wait_for_completion:
            mock_run.state.life_cycle_state.value = "TERMINATED"
            mock_run.state.result_state.value = "SUCCESS"
            mock_ws.jobs.run_now_and_wait.return_value = mock_run
        else:
            mock_ws.jobs.run_now.return_value = mock_run

        # Execute run
        result = runner.run(
            input_source=str(test_file),
            output_ws_folder="/Workspace/output",
            source_dialect="snowflake",
            catalog="cat",
            schema="sch",
            volume="vol",
            job_id=12345,
            switch_options={"log_level": "DEBUG"},
            wait_for_completion=wait_for_completion,
        )

        # Verify correct method called with proper job_parameters
        expected_params = {
            "result_catalog": "cat",
            "result_schema": "sch",
            "builtin_prompt": "snowflake",
            "log_level": "DEBUG",
        }

        if wait_for_completion:
            call_args = mock_ws.jobs.run_now_and_wait.call_args
            mock_ws.jobs.run_now.assert_not_called()
        else:
            call_args = mock_ws.jobs.run_now.call_args
            mock_ws.jobs.run_now_and_wait.assert_not_called()

        # Verify job_id and job_parameters
        assert call_args[0][0] == 12345  # job_id
        actual_params = call_args[1]["job_parameters"]
        assert actual_params["result_catalog"] == expected_params["result_catalog"]
        assert actual_params["result_schema"] == expected_params["result_schema"]
        assert actual_params["builtin_prompt"] == expected_params["builtin_prompt"]
        assert actual_params["log_level"] == expected_params["log_level"]
        assert "input_dir" in actual_params
        assert "output_dir" in actual_params

        # Verify result structure
        assert isinstance(result, list)
        assert len(result) == 1
        first_item = result[0]
        assert isinstance(first_item, dict)

        if wait_for_completion:
            assert first_item["state"] == "TERMINATED"
            assert first_item["result_state"] == "SUCCESS"
        else:
            assert "state" not in first_item
            assert "result_state" not in first_item

        # Verify common result fields
        assert first_item["job_id"] == 12345
        assert first_item["run_id"] == 99999
        # Verify run_url format
        assert first_item["run_url"] == "https://test.databricks.com/jobs/12345/runs/99999"

    @pytest.mark.parametrize(
        "wait_for_completion",
        (
            pytest.param(False, id="async_missing_run_id"),
            pytest.param(True, id="sync_missing_run_id"),
        ),
    )
    def test_run_job_execution_with_missing_run_id(
        self, runner: FriendOfSwitchRunner, mock_ws: Mock, tmp_path: Path, wait_for_completion: bool
    ) -> None:
        """Test SystemExit when run_id is missing."""
        # Setup test file
        test_file = tmp_path / "test.sql"
        test_file.write_text("SELECT 1;")

        # Mock job execution with missing run_id
        mock_run = Mock()
        mock_run.run_id = None  # Simulate missing run_id
        if wait_for_completion:
            mock_ws.jobs.run_now_and_wait.return_value = mock_run
        else:
            mock_ws.jobs.run_now.return_value = mock_run

        # Execute and expect SystemExit
        with pytest.raises(SystemExit, match="Job 12345 execution failed"):
            runner.run(
                input_source=str(test_file),
                output_ws_folder="/Workspace/output",
                source_dialect="snowflake",
                catalog="cat",
                schema="sch",
                volume="vol",
                job_id=12345,
                switch_options={},
                wait_for_completion=wait_for_completion,
            )
