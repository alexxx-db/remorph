import os
import json

import logging
from typing import Dict

from databricks.sdk.service.iam import User
from databricks.sdk import WorkspaceClient

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

    def __init__(self, ws: WorkspaceClient, current_user: User, is_debug: bool = False):
        self._ws = ws
        self._current_user = current_user
        self._dashboard_location = f"/Workspace/Users/{self._current_user}/Lakebridge/Dashboards"
        self._is_debug = is_debug

    def create_profiler_summary_dashboard(self, extract_file: str | None, source_tech: str | None) -> None:
        # TODO: check if the dashboard exists and unpublish it if it does
        # json_dashboard = DashboardTemplateLoader("templates").load(source_tech)

        # TODO: set the serialized dashboard JSON and warehouse ID
        self._ws.dashboards.create(
            name=self.DASHBOARD_NAME,
            dashboard_filters_enabled=None,
            is_favorite=False,
            parent=self._dashboard_location,
            run_as_role=None,
            tags=None,
        )

    def upload_duckdb_to_uc_volume(self, workspace_url, access_token, local_file_path, volume_path):
        """
        Upload a DuckDB file to Unity Catalog Volume using PUT method
        
        Args:
            workspace_url (str): Databricks workspace URL (e.g., 'https://your-workspace.cloud.databricks.com')
            access_token (str): Personal access token for authentication
            local_file_path (str): Local path to the DuckDB file
            volume_path (str): Target path in UC Volume (e.g., '/Volumes/catalog/schema/volume/myfile.duckdb')
            
        Returns:
            bool: True if successful, False otherwise
        """
        
        # Validate inputs
        if not os.path.exists(local_file_path):
            print(f"Error: Local file not found: {local_file_path}")
            return False
        
        if not volume_path.startswith('/Volumes/'):
            print("Error: Volume path must start with '/Volumes/'")
            return False
        
        headers = {
            'Authorization': f'Bearer {access_token}'
        }
        
        workspace_url = workspace_url.rstrip('/')
        
        try:
            # Use PUT method to upload directly to the volume path
            url = f"{workspace_url}/api/2.0/fs/files{volume_path}"
            
            with open(local_file_path, 'rb') as f:
                response = requests.put(url, headers=headers, data=f)
            
            if response.status_code in [200, 201, 204]:
                print(f"Successfully uploaded {local_file_path} to {volume_path}")
                return True
            else:
                print(f"Upload failed: {response.status_code} - {response.text}")
                return False
                
        except Exception as e:
            print(f"Upload failed: {str(e)}")
            return False
