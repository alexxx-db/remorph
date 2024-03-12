from io import StringIO

from databricks.labs.remorph.reconcile.connectors.databricks import DatabricksDataSource
from databricks.labs.remorph.reconcile.connectors.oracle import OracleDataSource
from databricks.labs.remorph.reconcile.connectors.snowflake import SnowflakeDataSource
from databricks.labs.remorph.reconcile.constants import (
    ColumnTransformationType,
    Constants,
    SourceType,
)
from databricks.labs.remorph.reconcile.recon_config import (
    ColumnMapping,
    Schema,
    Tables,
    Transformation,
    TransformRuleMapping,
)


class QueryBuilder:

    def __init__(self, table_conf: Tables, schema: list[Schema], layer: str, source: str):
        self.table_conf = table_conf
        self.schema = schema
        self.layer = layer
        self.source = source

    def build_hash_query(self) -> str:
        schema_info = {v.column_name: v for v in self.schema}

        columns, key_columns = self._get_column_list()
        col_transformations = self._generate_transformation_rule_mapping(columns, schema_info)

        hash_columns_expr = self._get_column_expr(
            TransformRuleMapping.get_column_expression_without_alias, col_transformations
        )
        hash_expr = self._generate_hash_algorithm(self.source, hash_columns_expr)

        key_column_transformation = filter(
            lambda transformation: transformation.column_name in key_columns, col_transformations
        )
        key_column_expr = self._get_column_expr(
            TransformRuleMapping.get_column_expression_with_alias, key_column_transformation
        )

        if self.layer == "source":
            table_name = self.table_conf.source_name
            query_filter = self.table_conf.filters.source if self.table_conf.filters else " 1 = 1"
        else:
            table_name = self.table_conf.target_name
            query_filter = self.table_conf.filters.target if self.table_conf.filters else " 1 = 1"

        # construct select query
        select_query = self._construct_hash_query(table_name, query_filter, hash_expr, key_column_expr)

        return select_query

    def _get_column_list(self):
        if self.layer == "source":
            join_columns = {col.source_name for col in self.table_conf.join_columns}
        else:
            join_columns = {col.target_name for col in self.table_conf.join_columns}

        if self.table_conf.select_columns is None:
            select_columns = {sch.column_name for sch in self.schema}
        else:
            select_columns = set(self.table_conf.select_columns)

        if self.table_conf.jdbc_reader_options:
            partition_column = {self.table_conf.jdbc_reader_options.partition_column}
        else:
            partition_column = set()

        # Combine all column names
        all_columns = join_columns | select_columns | partition_column

        # Remove threshold and drop columns
        threshold_columns = {thresh.column_name for thresh in self.table_conf.thresholds or []}
        drop_columns = set(self.table_conf.drop_columns or [])
        columns = all_columns - threshold_columns - drop_columns
        key_columns = join_columns | partition_column

        return columns, key_columns

    def _generate_transformation_rule_mapping(self, columns: set[str], schema: dict) -> list[TransformRuleMapping]:
        transformations_dict = self.table_conf.list_to_dict(Transformation, "column_name")
        column_mapping_dict = self.table_conf.list_to_dict(ColumnMapping, "target_name")

        transformation_rule_mapping = []
        for column in columns:
            if column in transformations_dict.keys():
                transformation = self._get_layer_transform(transformations_dict, column, self.layer)
            else:
                column_data_type = schema.get(column).data_type
                transformation = self._get_default_transformation(self.source, column_data_type).format(column)

            if column in column_mapping_dict.keys():
                column_alias = column_mapping_dict.get(column).source_name
            else:
                column_alias = column

            transformation_rule_mapping.append(TransformRuleMapping(column, transformation, column_alias))

        return transformation_rule_mapping

    @staticmethod
    def _get_default_transformation(data_source: str, data_type: str) -> str:
        match data_source:
            case "oracle":
                return OracleDataSource.oracle_datatype_mapper.get(
                    data_type, ColumnTransformationType.ORACLE_DEFAULT.value
                )
            case "snowflake":
                return SnowflakeDataSource.snowflake_datatype_mapper.get(
                    data_type, ColumnTransformationType.SNOWFLAKE_DEFAULT.value
                )
            case "databricks":
                return DatabricksDataSource.databricks_datatype_mapper.get(
                    data_type, ColumnTransformationType.DATABRICKS_DEFAULT.value
                )
            case _:
                msg = f"Unsupported source type --> {data_source}"
                raise ValueError(msg)

    @staticmethod
    def _get_layer_transform(transform_dict: dict[str, Transformation], column: str, layer: str) -> str:
        return transform_dict.get(column).source if layer == "source" else transform_dict.get(column).target

    @staticmethod
    def _get_column_expr(func, column_transformations: list[TransformRuleMapping]):
        return [func(transformation) for transformation in column_transformations]

    @staticmethod
    def _generate_hash_algorithm(source: str, column_expr: list[str]) -> str:
        if source in {SourceType.DATABRICKS.value, SourceType.SNOWFLAKE.value}:
            hash_expr = "concat(" + ", ".join(column_expr) + ")"
        else:
            hash_expr = " || ".join(column_expr)

        return (Constants.hash_algorithm_mapping.get(source.lower()).get("source")).format(hash_expr)

    @staticmethod
    def _construct_hash_query(table_name: str, query_filter: str, hash_expr: str, key_column_expr: list[str]) -> str:
        sql_query = StringIO()
        sql_query.write(f"select {hash_expr} as {Constants.hash_column_name}, ")

        # add join column
        sql_query.write(",".join(key_column_expr))
        sql_query.write(f" from {table_name} where {query_filter}")

        select_query = sql_query.getvalue()
        sql_query.close()
        return select_query
