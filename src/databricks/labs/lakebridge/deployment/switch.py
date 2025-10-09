import logging
import os
from pathlib import Path

from databricks.labs.blueprint.installation import Installation
from databricks.labs.blueprint.installer import InstallState
from databricks.labs.blueprint.wheels import ProductInfo
from databricks.labs.lakebridge.deployment.job import JobDeployment
from databricks.labs.lakebridge.config import SwitchResourcesConfig
from databricks.labs.lakebridge.transpiler.repository import TranspilerRepository
from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import InvalidParameterValue, NotFound
from databricks.sdk.service.jobs import JobParameterDefinition, JobSettings, NotebookTask, Source, Task

logger = logging.getLogger(__name__)


class SwitchDeployment:
    _INSTALL_STATE_KEY = "Switch"
    _TRANSPILER_ID = "switch"

    def __init__(
        self,
        ws: WorkspaceClient,
        installation: Installation,
        install_state: InstallState,
        product_info: ProductInfo,
        job_deployer: JobDeployment,
        transpiler_repository: TranspilerRepository,
    ):
        self._ws = ws
        self._installation = installation
        self._install_state = install_state
        self._product_info = product_info
        self._job_deployer = job_deployer
        self._transpiler_repository = transpiler_repository

    def install(self, switch_package_path: Path, resources: SwitchResourcesConfig) -> None:
        """Deploy Switch to workspace and configure resources."""
        logger.info("Deploying Switch to workspace...")
        self._deploy_workspace(switch_package_path)
        self._setup_job()
        self._record_resources(resources)
        logger.info("Switch deployment completed")

    def uninstall(self) -> None:
        """Remove Switch job from workspace."""
        if self._INSTALL_STATE_KEY not in self._install_state.jobs:
            logger.info("No Switch job found in InstallState")
            return

        try:
            job_id = int(self._install_state.jobs[self._INSTALL_STATE_KEY])
            logger.info(f"Removing Switch job with job_id={job_id}")
            del self._install_state.jobs[self._INSTALL_STATE_KEY]
            self._ws.jobs.delete(job_id)
            self._install_state.save()
        except (InvalidParameterValue, NotFound):
            logger.warning(f"Switch job {job_id} doesn't exist anymore")
            self._install_state.save()

    def get_configured_resources(self) -> dict[str, str] | None:
        """Get configured Switch resources (catalog, schema, volume)."""
        if self._install_state.switch_resources:
            return {
                "catalog": self._install_state.switch_resources.get("catalog"),
                "schema": self._install_state.switch_resources.get("schema"),
                "volume": self._install_state.switch_resources.get("volume"),
            }
        return None

    def _deploy_workspace(self, switch_package_dir: Path) -> None:
        """Deploy Switch package to workspace from site-packages."""
        try:
            logger.info("Deploying Switch package to workspace...")
            remote_path = f"{self._TRANSPILER_ID}/databricks"
            self._upload_directory(switch_package_dir, remote_path)
            logger.info("Switch workspace deployment completed")
        except (OSError, ValueError, AttributeError) as e:
            logger.error(f"Failed to deploy to workspace: {e}")

    def _upload_directory(self, local_path: Path, remote_prefix: str) -> None:
        """Recursively upload directory to workspace, excluding cache files."""
        for root, dirs, files in os.walk(local_path):
            # Skip cache directories and hidden directories
            dirs[:] = [d for d in dirs if d != "__pycache__" and not d.startswith(".")]

            for file in files:
                # Skip compiled Python files and hidden files
                if file.endswith((".pyc", ".pyo")) or file.startswith("."):
                    continue

                local_file = Path(root) / file
                rel_path = local_file.relative_to(local_path)
                remote_path = f"{remote_prefix}/{rel_path}"

                with open(local_file, "rb") as f:
                    content = f.read()

                self._installation.upload(remote_path, content)

    def _setup_job(self) -> None:
        """Create or update Switch job."""
        existing_job_id = self._get_existing_job_id()
        logger.info("Setting up Switch job in workspace...")
        try:
            job_id = self._create_or_update_switch_job(existing_job_id)
            self._install_state.jobs[self._INSTALL_STATE_KEY] = job_id
            self._install_state.save()
            job_url = f"{self._ws.config.host}/jobs/{job_id}"
            logger.info(f"Switch job created/updated: {job_url}")
        except (RuntimeError, ValueError, InvalidParameterValue) as e:
            logger.error(f"Failed to create/update Switch job: {e}")

    def _get_existing_job_id(self) -> str | None:
        """Check if Switch job already exists in workspace."""
        if self._INSTALL_STATE_KEY not in self._install_state.jobs:
            return None
        try:
            job_id = self._install_state.jobs[self._INSTALL_STATE_KEY]
            self._ws.jobs.get(int(job_id))
            return job_id
        except (InvalidParameterValue, NotFound, ValueError):
            return None

    def _create_or_update_switch_job(self, job_id: str | None) -> str:
        """Create or update Switch job, returning job ID."""
        job_settings = self._get_switch_job_settings()

        # Try to update existing job
        if job_id:
            try:
                logger.info(f"Updating Switch job: {job_id}")
                self._ws.jobs.reset(int(job_id), JobSettings(**job_settings))
                return job_id
            except (ValueError, InvalidParameterValue):
                logger.warning("Previous Switch job not found, creating new one")

        # Create new job
        logger.info("Creating new Switch job")
        new_job = self._ws.jobs.create(**job_settings)
        new_job_id = str(new_job.job_id)
        assert new_job_id is not None
        return new_job_id

    def _get_switch_job_settings(self) -> dict:
        """Build job settings for Switch transpiler."""
        product = self._installation.product()
        job_name = f"{product.upper()}_Switch"
        version = ProductInfo.from_class(self.__class__).version()
        user_name = self._installation.username()
        notebook_path = (
            f"/Workspace/Users/{user_name}/.{product}/{self._TRANSPILER_ID}/"
            f"databricks/labs/switch/notebooks/00_main"
        )

        task = Task(
            task_key="run_transpilation",
            notebook_task=NotebookTask(
                notebook_path=notebook_path,
                source=Source.WORKSPACE,
            ),
            disable_auto_optimization=True,  # To disable retries on failure
        )

        return {
            "name": job_name,
            "tags": {"created_by": user_name, "switch_version": f"v{version}"},
            "tasks": [task],
            "parameters": self._get_switch_job_parameters(),
            "max_concurrent_runs": 100,  # Allow simultaneous transpilations
        }

    def _get_switch_job_parameters(self) -> list[JobParameterDefinition]:
        """Build job-level parameter definitions from installed config.yml."""
        configs = self._transpiler_repository.all_transpiler_configs()
        config = configs.get(self._INSTALL_STATE_KEY) or configs.get(self._TRANSPILER_ID)

        if not config:
            raise ValueError(
                "Switch config.yml not found. This indicates an incomplete installation. "
                "Please reinstall Switch transpiler."
            )

        # Add required runtime parameters not in config at the beginning
        parameters = {
            "input_dir": "",
            "output_dir": "",
            "result_catalog": "",
            "result_schema": "",
            "builtin_prompt": "",
        }

        # Then add parameters from config.yml
        for option in config.options.get("all", []):
            flag = option.flag
            default = option.default or ""

            # Convert special values
            if default == "<none>":
                default = ""
            elif isinstance(default, (int, float)):
                default = str(default)

            parameters[flag] = default

        return [JobParameterDefinition(name=key, default=value) for key, value in parameters.items()]

    def _record_resources(self, resources: SwitchResourcesConfig) -> None:
        """Persist configured Switch resources for later reuse."""
        self._install_state.switch_resources["catalog"] = resources.catalog
        self._install_state.switch_resources["schema"] = resources.schema
        self._install_state.switch_resources["volume"] = resources.volume
        self._install_state.save()
        logger.info(
            f"Switch resources stored: catalog=`{resources.catalog}`, "
            f"schema=`{resources.schema}`, volume=`{resources.volume}`"
        )
