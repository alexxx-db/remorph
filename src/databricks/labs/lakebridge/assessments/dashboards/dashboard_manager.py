import io
import os
import json

import logging
from typing import Dict

from databricks.sdk.service.iam import User
from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import PermissionDenied, NotFound, InternalError

from databricks.labs.lakebridge.deployment.dashboard import DashboardDeployment
from databricks.labs.blueprint.wheels import find_project_root

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class DashboardTemplateLoader:
    """
    Class for loading the JSON representation of a Databricks dashboard
    according to the source system.
    """

    def __init__(self, templates_dir: str = "templates"):
        self.templates_dir = templates_dir

    def load(self, source_system: str) -> Dict:
        """
        Loads a profiler summary dashboard.
        :param source_system: - the name of the source data warehouse
        """
        filename = f"{source_system.lower()}_dashboard.json"
        filepath = os.path.join(self.templates_dir, filename)
        if not os.path.exists(filepath):
            raise FileNotFoundError(f"Could not find dashboard template matching '{source_system}'.")
        with open(filepath, "r", encoding="utf-8") as f:
            return json.load(f)


class DashboardManager:
    """
    Class for managing the lifecycle of a profiler dashboard summary, a.k.a. "local dashboards"
    """

    DASHBOARD_NAME = "Lakebridge Profiler Assessment"

    def __init__(self, ws: WorkspaceClient, current_user: User, dashboard_deployer: DashboardDeployment, is_debug: bool = False):
        self._ws = ws
        self._current_user = current_user
        self._dashboard_deployer = dashboard_deployer
        self._dashboard_location = f"/Workspace/Users/{self._current_user}/Lakebridge/Dashboards"
        self._is_debug = is_debug

    def create_profiler_summary_dashboard(self, extract_file: str | None, source_tech: str | None) -> None:
        # TODO: check if the dashboard exists and unpublish it if it does
        # TODO: set the serialized dashboard JSON and warehouse ID
        logger.info("Deploying profiler summary dashboard.")
        dashboard_base_dir = (
            find_project_root(__file__) / f"src/databricks/labs/lakebridge/resources/assessments/dashboards/{source_tech}"
        )
        self._dashboard_deployer.deploy(dashboard_base_dir, recon_config)

        self._ws.dashboards.create(
            name=self.DASHBOARD_NAME,
            dashboard_filters_enabled=None,
            is_favorite=False,
            parent=self._dashboard_location,
            run_as_role=None,
            tags=None,
        )

    def upload_duckdb_to_uc_volume(self, local_file_path, volume_path):
        """
        Upload a DuckDB file to Unity Catalog Volume

        Args:
            local_file_path (str): Local path to the DuckDB file
            volume_path (str): Target path in UC Volume (e.g., '/Volumes/catalog/schema/volume/myfile.duckdb')

        Returns:
            bool: True if successful, False otherwise
        """

        # Validate inputs
        if not os.path.exists(local_file_path):
            logger.error(f"Local file not found: {local_file_path}")
            return False

        if not volume_path.startswith('/Volumes/'):
            logger.error("Volume path must start with '/Volumes/'")
            return False

        try:
            with open(local_file_path, 'rb') as f:
                file_bytes = f.read()
                binary_data = io.BytesIO(file_bytes)
                self._ws.files.upload(volume_path, binary_data, overwrite=True)
            logger.info(f"Successfully uploaded {local_file_path} to {volume_path}")
            return True
        except FileNotFoundError as e:
            logger.error(f"Profiler extract file was not found: \n{e}")
            return False
        except PermissionDenied as e:
            logger.error(f"Insufficient privileges detected while accessing Volume path: \n{e}")
            return False
        except NotFound as e:
            logger.error(f"Invalid Volume path provided: \n{e}")
            return False
        except InternalError as e:
            logger.error(f"Internal Databricks error while uploading extract file: \n{e}")
            return False
        except Exception as e:
            logger.error(f"Failed to upload file: {str(e)}")
            return False
