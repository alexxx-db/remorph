import logging
from dataclasses import dataclass
from pathlib import Path

from databricks.labs.remorph.transpiler.transpile_status import TranspileError
from databricks.labs.remorph.reconcile.recon_config import Table
<<<<<<< HEAD
=======
<<<<<<< HEAD
>>>>>>> databrickslabs-main


logger = logging.getLogger(__name__)

<<<<<<< HEAD
=======
=======
from databricks.labs.remorph.snow import databricks, oracle, snowflake, presto

logger = logging.getLogger(__name__)

SQLGLOT_DIALECTS: dict[str, DialectType] = {
    "athena": Dialects.ATHENA,
    "bigquery": Dialects.BIGQUERY,
    "databricks": databricks.Databricks,
    "mysql": Dialects.MYSQL,
    "netezza": Dialects.POSTGRES,
    "oracle": oracle.Oracle,
    "postgresql": Dialects.POSTGRES,
    "presto": presto.Presto,
    "redshift": Dialects.REDSHIFT,
    "snowflake": snowflake.Snow,
    "sqlite": Dialects.SQLITE,
    "teradata": Dialects.TERADATA,
    "trino": Dialects.TRINO,
    "tsql": Dialects.TSQL,
    "vertica": Dialects.POSTGRES,
}


def get_dialect(engine: str) -> Dialect:
    return Dialect.get_or_raise(SQLGLOT_DIALECTS.get(engine))


def get_key_from_dialect(input_dialect: Dialect) -> str:
    return [source_key for source_key, dialect in SQLGLOT_DIALECTS.items() if dialect == input_dialect][0]

>>>>>>> 30dc687c (Added support for `PARSE_JSON` and `VARIANT` datatype (#906))
>>>>>>> databrickslabs-main

@dataclass
class TranspileConfig:
    __file__ = "config.yml"
<<<<<<< HEAD
    __version__ = 2
=======
    __version__ = 3
>>>>>>> databrickslabs-main

    transpiler_config_path: str
    source_dialect: str
    input_source: str | None = None
    output_folder: str | None = None
<<<<<<< HEAD
=======
    error_file_path: str | None = None
>>>>>>> databrickslabs-main
    sdk_config: dict[str, str] | None = None
    skip_validation: bool = False
    catalog_name: str = "remorph"
    schema_name: str = "transpiler"
<<<<<<< HEAD
    mode: str = "current"
=======
>>>>>>> databrickslabs-main

    @property
    def transpiler_path(self):
        return Path(self.transpiler_config_path)

    @property
    def input_path(self):
        if self.input_source is None:
            raise ValueError("Missing input source!")
        return Path(self.input_source)

    @property
    def output_path(self):
        return None if self.output_folder is None else Path(self.output_folder)

    @property
<<<<<<< HEAD
    def target_dialect(self):
        return "experimental" if self.mode == "experimental" else "databricks"
=======
    def error_path(self):
        return Path(self.error_file_path) if self.error_file_path else None

    @property
    def target_dialect(self):
        return "databricks"
>>>>>>> databrickslabs-main


@dataclass
class TableRecon:
    __file__ = "recon_config.yml"
    __version__ = 1

    source_schema: str
    target_catalog: str
    target_schema: str
    tables: list[Table]
    source_catalog: str | None = None

    def __post_init__(self):
        self.source_schema = self.source_schema.lower()
        self.target_schema = self.target_schema.lower()
        self.target_catalog = self.target_catalog.lower()
        self.source_catalog = self.source_catalog.lower() if self.source_catalog else self.source_catalog


@dataclass
class DatabaseConfig:
    source_schema: str
    target_catalog: str
    target_schema: str
    source_catalog: str | None = None


@dataclass
class TranspileResult:
    transpiled_code: str
    success_count: int
    error_list: list[TranspileError]


@dataclass
class ValidationResult:
    validated_sql: str
    exception_msg: str | None


@dataclass
class ReconcileTablesConfig:
    filter_type: str  # all/include/exclude
    tables_list: list[str]  # [*, table1, table2]


@dataclass
class ReconcileMetadataConfig:
    catalog: str = "remorph"
    schema: str = "reconcile"
    volume: str = "reconcile_volume"


@dataclass
class ReconcileConfig:
    __file__ = "reconcile.yml"
    __version__ = 1

    data_source: str
    report_type: str
    secret_scope: str
    database_config: DatabaseConfig
    metadata_config: ReconcileMetadataConfig
    job_id: str | None = None
    tables: ReconcileTablesConfig | None = None


@dataclass
class RemorphConfigs:
    transpile: TranspileConfig | None = None
    reconcile: ReconcileConfig | None = None
