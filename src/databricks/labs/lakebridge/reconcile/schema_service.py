from databricks.labs.lakebridge.config import DatabaseConfig
from databricks.labs.lakebridge.reconcile.connectors.data_source import DataSource
from databricks.labs.lakebridge.reconcile.recon_config import Schema, Table


class SchemaService:

    @staticmethod
    def get_schemas(
        source: DataSource,
        target: DataSource,
        table_conf: Table,
        database_config: DatabaseConfig,
    ) -> tuple[list[Schema], list[Schema]]:
        src_schema = source.get_schema(
            catalog=database_config.source_catalog,
            schema=database_config.source_schema,
            table=table_conf.source_name,
        )

        tgt_schema = target.get_schema(
            catalog=database_config.target_catalog,
            schema=database_config.target_schema,
            table=table_conf.target_name,
        )

        return src_schema, tgt_schema
