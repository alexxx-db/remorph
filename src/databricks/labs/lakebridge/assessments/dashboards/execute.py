import logging
import os
import sys

import duckdb
from pyspark.sql import SparkSession

from databricks.labs.lakebridge.assessments.profiler_validator import EmptyTableValidationCheck, build_validation_report

logger = logging.getLogger(__name__)


def main(*argv) -> None:
    logger.debug(f"Arguments received: {argv}")
    assert len(sys.argv) == 4, f"Invalid number of arguments: {len(sys.argv)}"
    catalog_name = sys.argv[0]
    schema_name = sys.argv[1]
    extract_location = sys.argv[2]
    source_tech = sys.argv[3]
    logger.info(f"Validating {source_tech} profiler extract located at '{extract_location}'.")
    valid_extract = _validate_profiler_extract(extract_location)
    if valid_extract:
        _ingest_profiler_tables(catalog_name, schema_name, extract_location)
    else:
        raise ValueError("Corrupt or invalid profiler extract.")


def _validate_profiler_extract(extract_location: str) -> bool:
    logger.info("Validating the profiler extract file.")
    validation_checks = []
    try:
        with duckdb.connect(database=extract_location) as duck_conn:
            tables = duck_conn.execute("SHOW ALL TABLES").fetchall()
            for table in tables:
                fq_table_name = f"{table[0]}.{table[1]}.{table[2]}"
                empty_check = EmptyTableValidationCheck(fq_table_name)
                validation_checks.append(empty_check)
            report = build_validation_report(validation_checks, duck_conn)
    except duckdb.IOException as e:
        logger.exception(f"Could not access the profiler extract: '{extract_location}'.")
        raise e
    except Exception as e:
        logger.exception(f"Unable to validate the profiler extract: '{extract_location}'.")
        raise e

    if len(report) > 0:
        report_errors = list(filter(lambda x: x.outcome == "FAIL" and x.severity == "ERROR", report))
        num_errors = len(report_errors)
        logger.info(f"There are {num_errors} validation errors in the profiler extract.")
        for error in report_errors:
            logging.info(error)
    else:
        raise ValueError("Profiler extract validation report is empty.")
    return num_errors == 0


def _ingest_profiler_tables(catalog_name: str, schema_name: str, extract_location: str) -> None:
    try:
        with duckdb.connect(database=extract_location) as duck_conn:
            tables_to_ingest = duck_conn.execute("SHOW ALL TABLES").fetchall()
    except duckdb.IOException as e:
        logger.error(f"Could not access the profiler extract: '{extract_location}': {e}")
        raise duckdb.IOException(f"Could not access the profiler extract: '{extract_location}'.") from e
    except Exception as e:
        logger.error(f"Unable to read tables from profiler extract: '{extract_location}': {e}")
        raise e

    if len(tables_to_ingest) == 0:
        raise ValueError("Profiler extract contains no tables.")

    successful_tables = []
    unsuccessful_tables = []
    for source_table in tables_to_ingest:
        try:
            fq_source_table_name = f"{source_table[0]}.{source_table[1]}.{source_table[2]}"
            fq_delta_table_name = f"{catalog_name}.{schema_name}.{source_table[2]}"
            logger.info(f"Ingesting profiler table: '{fq_source_table_name}'")
            _ingest_table(extract_location, fq_source_table_name, fq_delta_table_name)
            successful_tables.append(fq_source_table_name)
        except (ValueError, IndexError, TypeError) as e:
            logger.error(f"Failed to construct source and destination table names: {e}")
            unsuccessful_tables.append(source_table)
        except duckdb.Error as e:
            logger.error(f"Failed to ingest table from profiler database: {e}")
            unsuccessful_tables.append(source_table)
    logger.info(f"Ingested {len(successful_tables)} tables from profiler extract.")
    logger.info(",".join(successful_tables))
    logger.info(f"Failed to ingest {len(unsuccessful_tables)} tables from profiler extract.")
    logger.info(",".join(unsuccessful_tables))


def _ingest_table(extract_location: str, source_table_name: str, target_table_name: str) -> None:
    """
    Ingest a table from a DuckDB profiler extract into a managed Delta table in Unity Catalog.
    """
    try:
        with duckdb.connect(database=extract_location, read_only=True) as duck_conn:
            query = f"SELECT * FROM {source_table_name}"
            pdf = duck_conn.execute(query).df()
            # Save table as a managed Delta table in Unity Catalog
            logger.info(f"Saving profiler table '{target_table_name}' to Unity Catalog.")
            spark = SparkSession.builder.getOrCreate()
            df = spark.createDataFrame(pdf)
            df.write.format("delta").mode("overwrite").saveAsTable(target_table_name)
    except duckdb.CatalogException as e:
        logger.error(f"Could not find source table '{source_table_name}' in profiler extract: {e}")
        raise duckdb.CatalogException(f"Could not find source table '{source_table_name}' in profiler extract.") from e
    except duckdb.IOException as e:
        logger.error(f"Could not access the profiler extract: '{extract_location}': {e}")
        raise duckdb.IOException(f"Could not access the profiler extract: '{extract_location}'.") from e
    except Exception as e:
        logger.error(f"Unable to ingest table '{source_table_name}' from profiler extract: {e}")
        raise e


if __name__ == "__main__":
    # Ensure that the ingestion job is being run on a Databricks cluster
    if "DATABRICKS_RUNTIME_VERSION" not in os.environ:
        raise SystemExit("The Lakebridge profiler ingestion job is only intended to run in a Databricks Runtime.")
    main(*sys.argv)
