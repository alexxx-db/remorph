from pyspark.sql import SparkSession
from sqlglot import Dialect
<<<<<<< HEAD
=======
from sqlglot.dialects import TSQL
>>>>>>> databrickslabs-main

from databricks.labs.remorph.reconcile.connectors.data_source import DataSource
from databricks.labs.remorph.reconcile.connectors.databricks import DatabricksDataSource
from databricks.labs.remorph.reconcile.connectors.oracle import OracleDataSource
from databricks.labs.remorph.reconcile.connectors.snowflake import SnowflakeDataSource
<<<<<<< HEAD
=======
from databricks.labs.remorph.reconcile.connectors.sql_server import SQLServerDataSource
>>>>>>> databrickslabs-main
from databricks.labs.remorph.transpiler.sqlglot.generator.databricks import Databricks
from databricks.labs.remorph.transpiler.sqlglot.parsers.oracle import Oracle
from databricks.labs.remorph.transpiler.sqlglot.parsers.snowflake import Snowflake
from databricks.sdk import WorkspaceClient


def create_adapter(
    engine: Dialect,
    spark: SparkSession,
    ws: WorkspaceClient,
    secret_scope: str,
) -> DataSource:
    if isinstance(engine, Snowflake):
        return SnowflakeDataSource(engine, spark, ws, secret_scope)
    if isinstance(engine, Oracle):
        return OracleDataSource(engine, spark, ws, secret_scope)
    if isinstance(engine, Databricks):
        return DatabricksDataSource(engine, spark, ws, secret_scope)
<<<<<<< HEAD
=======
    if isinstance(engine, TSQL):
        return SQLServerDataSource(engine, spark, ws, secret_scope)
>>>>>>> databrickslabs-main
    raise ValueError(f"Unsupported source type --> {engine}")
