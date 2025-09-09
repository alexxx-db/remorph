from unittest.mock import create_autospec

from databricks.labs.blueprint.installation import MockInstallation
from databricks.labs.blueprint.installer import InstallState
from databricks.labs.blueprint.wheels import ProductInfo
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.jobs import Job

from databricks.labs.lakebridge.config import LakebridgeConfiguration
from databricks.labs.lakebridge.deployment.job import JobDeployment


def test_deploy_extract_ingestion_job():
    workspace_client = create_autospec(WorkspaceClient)
    job = Job(job_id=9771)
    workspace_client.jobs.create.return_value = job
    installation = MockInstallation(is_global=False)
    install_state = InstallState.from_installation(installation)
    product_info = ProductInfo.from_class(LakebridgeConfiguration)
    job_deployer = JobDeployment(workspace_client, installation, install_state, product_info)
    job_name = "Lakebridge - Profiler Ingestion Job"
    job_deployer.deploy_profiler_ingestion_job(
        name=job_name,
        source_tech="synapse",
        databricks_user="john.doe@example.com",
        volume_upload_location="/Volumes/lakebridge_profiler/profiler_runs/synapse_assessment.db",
        target_catalog="lakebridge",
    )
    workspace_client.jobs.create.assert_called_once()
    assert install_state.jobs[job_name] == str(job.job_id)
