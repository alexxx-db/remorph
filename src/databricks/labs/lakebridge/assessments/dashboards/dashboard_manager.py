import os
import json

import requests
import logging
from typing import Dict, Any

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

    def __init__(self, workspace_url: str, token: str, warehouse_id: str, databricks_username: str):
        self.warehouse_id = warehouse_id
        self.token = token
        if not workspace_url.startswith("http"):
            workspace_url = f"https://{workspace_url}"
        self.workspace_url = workspace_url.rstrip("/")
        self.session = requests.Session()
        self.session.headers.update({"Authorization": f"Bearer {token}", "Content-Type": "application/json"})
        self.databricks_username = databricks_username
        self.dashboard_location = f"/Workspace/Users/{databricks_username}/Lakebridge/Dashboards"
        self.dashboard_name = "Lakebridge Profiler Assessment"

    def _handle_response(self, resp: requests.Response) -> Dict[str, Any]:
        """Handle API responses with logging and error handling."""
        try:
            resp.raise_for_status()
            if resp.status_code == 204:
                return {"status": "success", "message": "No content"}
            return resp.json()
        except requests.exceptions.HTTPError as e:
            logger.error("API call failed: %s - %s", resp.status_code, resp.text)
            raise RuntimeError(f"Databricks API Error {resp.status_code}: {resp.text}") from e
        except Exception:
            logger.exception("Unexpected error during API call")
            raise

    def draft_dashboard(
        self, display_name: str, serialized_dashboard: str, parent_path: str, warehouse_id: str
    ) -> Dict[str, Any]:
        """Create a new dashboard in Databricks Lakeview."""
        url = f"{self.workspace_url}/api/2.0/lakeview/dashboards"
        payload = {
            "display_name": display_name,
            "warehouse_id": warehouse_id,
            "serialized_dashboard": serialized_dashboard,
            "parent_path": parent_path,
        }
        resp = self.session.post(url, json=payload)
        return self._handle_response(resp)

    def delete_dashboard(self, dashboard_id: str) -> Dict[str, Any]:
        """Delete a dashboard by ID."""
        url = f"{self.workspace_url}/api/2.0/lakeview/dashboards/{dashboard_id}"
        resp = self.session.delete(url)
        return self._handle_response(resp)

    def publish_dashboard(self, dashboard_id: str) -> Dict[str, Any]:
        """Publish a dashboard by ID."""
        url = f"{self.workspace_url}/api/2.0/lakeview/dashboards/{dashboard_id}/published"
        resp = self.session.post(url)
        return self._handle_response(resp)

    def unpublish_dashboard(self, dashboard_id: str) -> Dict[str, Any]:
        """Unpublish a dashboard by ID."""
        url = f"{self.workspace_url}/api/2.0/lakeview/dashboards/{dashboard_id}/published"
        resp = self.session.delete(url)
        return self._handle_response(resp)

    def get_unpublished_dashboard_serialized(self, dashboard_id: str) -> str:
        """
        Get the serialized_dashboard of an unpublished dashboard.

        Workflow:
        - First unpublish the dashboard
        - Then fetch the dashboard details
        """
        logger.info("Unpublishing dashboard %s before fetching details", dashboard_id)
        self.unpublish_dashboard(dashboard_id)

        url = f"{self.workspace_url}/api/2.0/lakeview/dashboards/{dashboard_id}"
        resp = self.session.get(url)
        data = self._handle_response(resp)

        serialized = data.get("serialized_dashboard")
        if not serialized:
            raise RuntimeError(f"Dashboard {dashboard_id} has no serialized_dashboard field")
        return serialized

    def create_profiler_summary_dashboard(self, source_system: str):
        # TODO: check if the dashboard exists
        # if it does, unpublish it and delete
        # create new dashboard
        json_dashboard = DashboardTemplateLoader("templates").load(source_system)
        dashboard_manager = DashboardManager(
            self.workspace_url, self.token, self.warehouse_id, self.databricks_username
        )
        response = dashboard_manager.draft_dashboard(
            dashboard_manager.dashboard_name,
            json.dumps(json_dashboard),
            parent_path=dashboard_manager.dashboard_location,
            warehouse_id=dashboard_manager.warehouse_id,
        )
        return response.get("dashboard_id")
