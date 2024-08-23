import asyncio
import re
from pathlib import Path
from typing import Any, cast
from unittest.mock import create_autospec, patch

import pytest

from databricks.connect import DatabricksSession
from databricks.labs.lsql.backends import MockBackend
from databricks.labs.lsql.core import Row
from databricks.sdk import WorkspaceClient

from databricks.labs.remorph.config import TranspileConfig, ValidationResult
from databricks.labs.remorph.helpers.file_utils import dir_walk, is_sql_file
from databricks.labs.remorph.helpers.validation import Validator
from databricks.labs.remorph.transpiler.execute import (
    transpile as do_transpile,
    transpile_column_exp,
    transpile_sql,
)
from databricks.sdk.core import Config

from databricks.labs.remorph.transpiler.sqlglot.sqlglot_engine import SqlglotEngine


# pylint: disable=unspecified-encoding


def transpile(workspace_client: WorkspaceClient, engine: SqlglotEngine, config: TranspileConfig):
    return asyncio.run(do_transpile(workspace_client, engine, config))


def check_status(
    status: dict[str, Any],
    total_files_processed: int,
    total_queries_processed: int,
    analysis_error_count: int,
    parsing_error_count: int,
    validation_error_count: int,
    generation_error_count: int,
    error_file_name: str,
):
    assert status is not None, "Status returned by transpile function is None"
    assert isinstance(status, dict), "Status returned by transpile function is not a dict"
    assert len(status) > 0, "Status returned by transpile function is an empty dict"
    assert (
        status["total_files_processed"] == total_files_processed
    ), "total_files_processed does not match expected value"
    assert (
        status["total_queries_processed"] == total_queries_processed
    ), "total_queries_processed does not match expected value"
    assert status["analysis_error_count"] == analysis_error_count, "analysis_error_count does not match expected value"

    assert status["parsing_error_count"] == parsing_error_count, "parsing_error_count does not match expected value"
    assert (
        status["validation_error_count"] == validation_error_count
    ), "validation_error_count does not match expected value"
    assert (
        status["generation_error_count"] == generation_error_count
    ), "generation_error_count does not match expected value"
    assert status["error_log_file"], "error_log_file is None or empty"
    assert Path(status["error_log_file"]).name == error_file_name, f"error_log_file does not match {error_file_name}'"


<<<<<<< HEAD
def check_error_lines(error_file_path: str, expected_errors: list[dict[str, str]]):
    pattern = r"TranspileError\(code=(?P<code>[^,]+), kind=(?P<kind>[^,]+), severity=(?P<severity>[^,]+), path='(?P<path>[^']+)', message='(?P<message>[^']+)('\))?"
    with open(Path(error_file_path)) as file:
        error_count = 0
        match_count = 0
=======
def write_data_to_file(path: Path, content: str):
    with path.open("w") as writable:
        writable.write(content)


@pytest.fixture
def initial_setup(tmp_path: Path):
    input_dir = tmp_path / "remorph_transpile"
    query_1_sql_file = input_dir / "query1.sql"
    query_2_sql_file = input_dir / "query2.sql"
    query_3_sql_file = input_dir / "query3.sql"
    stream_1_sql_file = input_dir / "stream1.sql"
    call_center_ddl_file = input_dir / "call_center.ddl"
    file_text = input_dir / "file.txt"
    safe_remove_dir(input_dir)
    make_dir(input_dir)

    query_1_sql = """select  i_manufact, sum(ss_ext_sales_price) ext_price from date_dim, store_sales where
    d_date_sk = ss_sold_date_sk and substr(ca_zip,1,5) <> substr(s_zip,1,5) group by i_manufact order by i_manufact
    limit 100 ;"""

    call_center_ddl = """create table call_center
        (
            cc_call_center_sk         int                           ,
            cc_call_center_id         varchar(16)
        )

         CLUSTER BY(cc_call_center_sk)
         """

    query_2_sql = """select wswscs.d_week_seq d_week_seq1,sun_sales sun_sales1,mon_sales mon_sales1 from wswscs,
    date_dim where date_dim.d_week_seq = wswscs.d_week_seq and d_year = 2001"""

    query_3_sql = """with wscs as
     (select sold_date_sk
            ,sales_price
      from (select ws_sold_date_sk sold_date_sk
                  ,ws_ext_sales_price sales_price
            from web_sales
            union all
            select cs_sold_date_sk sold_date_sk
                  ,cs_ext_sales_price sales_price
            from catalog_sales)),
     wswscs as
     (select d_week_seq,
            sum(case when (d_day_name='Sunday') then sales_price else null end) sun_sales,
            sum(case when (d_day_name='Monday') then sales_price else null end) mon_sales,
            sum(case when (d_day_name='Tuesday') then sales_price else  null end) tue_sales,
            sum(case when (d_day_name='Wednesday') then sales_price else null end) wed_sales,
            sum(case when (d_day_name='Thursday') then sales_price else null end) thu_sales,
            sum(case when (d_day_name='Friday') then sales_price else null end) fri_sales,
            sum(case when (d_day_name='Saturday') then sales_price else null end) sat_sales
     from wscs
         ,date_dim
     where d_date_sk = sold_date_sk
     group by d_week_seq)
     select d_week_seq1
           ,round(sun_sales1/sun_sales2,2)
           ,round(mon_sales1/mon_sales2,2)
           ,round(tue_sales1/tue_sales2,2)
           ,round(wed_sales1/wed_sales2,2)
           ,round(thu_sales1/thu_sales2,2)
           ,round(fri_sales1/fri_sales2,2)
           ,round(sat_sales1/sat_sales2,2)
     from
     (select wswscs.d_week_seq d_week_seq1
            ,sun_sales sun_sales1
            ,mon_sales mon_sales1
            ,tue_sales tue_sales1
            ,wed_sales wed_sales1
            ,thu_sales thu_sales1
            ,fri_sales fri_sales1
            ,sat_sales sat_sales1
      from wswscs,date_dim
      where date_dim.d_week_seq = wswscs.d_week_seq and
            d_year = 2001) y,
     (select wswscs.d_week_seq d_week_seq2
            ,sun_sales sun_sales2
            ,mon_sales mon_sales2
            ,tue_sales tue_sales2
            ,wed_sales wed_sales2
            ,thu_sales thu_sales2
            ,fri_sales fri_sales2
            ,sat_sales sat_sales2
      from wswscs
          ,date_dim
      where date_dim.d_week_seq = wswscs.d_week_seq2 and
            d_year = 2001+1) z
     where d_week_seq1=d_week_seq2-53
     order by d_week_seq1;
     """

    stream_1_sql = """CREATE STREAM unsupported_stream AS SELECT * FROM some_table;"""

    write_data_to_file(query_1_sql_file, query_1_sql)
    write_data_to_file(call_center_ddl_file, call_center_ddl)
    write_data_to_file(query_2_sql_file, query_2_sql)
    write_data_to_file(query_3_sql_file, query_3_sql)
    write_data_to_file(stream_1_sql_file, stream_1_sql)
    write_data_to_file(file_text, "This is a test file")

    return input_dir


def test_with_dir_skip_validation(initial_setup, mock_workspace_client):
    input_dir = initial_setup
    config = MorphConfig(
        input_sql=str(input_dir),
        output_folder="None",
        sdk_config=None,
        source="snowflake",
        skip_validation=True,
    )

    # call morph
    with patch('databricks.labs.remorph.helpers.db_sql.get_sql_backend', return_value=MockBackend()):
        status = morph(mock_workspace_client, config)
    # assert the status
    assert status is not None, "Status returned by morph function is None"
    assert isinstance(status, list), "Status returned by morph function is not a list"
    assert len(status) > 0, "Status returned by morph function is an empty list"
    for stat in status:
        assert stat["total_files_processed"] == 6, "total_files_processed does not match expected value"
        assert stat["total_queries_processed"] == 5, "total_queries_processed does not match expected value"
        assert (
            stat["no_of_sql_failed_while_parsing"] == 0
        ), "no_of_sql_failed_while_parsing does not match expected value"
        assert (
            stat["no_of_sql_failed_while_validating"] == 1
        ), "no_of_sql_failed_while_validating does not match expected value"
        assert stat["error_log_file"], "error_log_file is None or empty"
        assert Path(stat["error_log_file"]).name.startswith("err_") and Path(stat["error_log_file"]).name.endswith(
            ".lst"
        ), "error_log_file does not match expected pattern 'err_*.lst'"

    expected_file_name = f"{input_dir}/query3.sql"
    expected_exception = f"Unsupported operation found in file {input_dir}/query3.sql."
    pattern = r"ValidationError\(file_name='(?P<file_name>[^']+)', exception='(?P<exception>[^']+)'\)"

    with open(Path(status[0]["error_log_file"])) as file:
>>>>>>> 96c6764d (Added Translation Support for `!` as `commands` and `&` for `Parameters` (#771))
        for line in file:
            match = re.match(pattern, line)
            if not match:
                continue
            error_count += 1
            # Extract information using group names from the pattern
            error_info = match.groupdict()
            # Perform assertions
            for expected_error in expected_errors:
                if expected_error["path"] == error_info["path"]:
                    match_count += 1
                    expected_message = expected_error["message"]
                    actual_message = error_info["message"]
                    assert (
                        expected_message in actual_message
                    ), f"Message {actual_message} does not match the expected value {expected_message}"
        assert match_count == len(expected_errors), "Not all expected errors were found"
        assert error_count == match_count, "Not all actual errors were matched"


def check_generated(input_source: Path, output_folder: Path):
    for _, _, files in dir_walk(input_source):
        for input_file in files:
            if not is_sql_file(input_file):
                continue
            relative = cast(Path, input_file).relative_to(input_source)
            transpiled = output_folder / relative
            assert transpiled.exists(), f"Could not find transpiled file {transpiled!s} for {input_file!s}"


def test_with_dir_with_output_folder_skipping_validation(
    input_source, output_folder, error_file, mock_workspace_client
):
    config = TranspileConfig(
        transpiler_config_path="sqlglot",
        input_source=str(input_source),
        output_folder=str(output_folder),
        error_file_path=str(error_file),
        sdk_config=None,
        source_dialect="snowflake",
        skip_validation=True,
    )
    with patch('databricks.labs.remorph.helpers.db_sql.get_sql_backend', return_value=MockBackend()):
<<<<<<< HEAD
        status, _errors = transpile(mock_workspace_client, SqlglotEngine(), config)
    # check the status
    check_status(status, 8, 7, 1, 2, 0, 0, error_file.name)
    # check errors
    expected_errors = [
        {
            "path": f"{input_source!s}/query3.sql",
            "message": f"Unsupported operation found in file {input_source!s}/query3.sql.",
        },
        {"path": f"{input_source!s}/query4.sql", "message": "Parsing error Start:"},
        {"path": f"{input_source!s}/query5.sql", "message": "Token error Start:"},
    ]
    check_error_lines(status["error_log_file"], expected_errors)
    # check generation
    check_generated(input_source, output_folder)
=======
        status = morph(mock_workspace_client, config)
    # assert the status
    assert status is not None, "Status returned by morph function is None"
    assert isinstance(status, list), "Status returned by morph function is not a list"
    assert len(status) > 0, "Status returned by morph function is an empty list"
    for stat in status:
        assert stat["total_files_processed"] == 6, "total_files_processed does not match expected value"
        assert stat["total_queries_processed"] == 5, "total_queries_processed does not match expected value"
        assert (
            stat["no_of_sql_failed_while_parsing"] == 0
        ), "no_of_sql_failed_while_parsing does not match expected value"
        assert (
            stat["no_of_sql_failed_while_validating"] == 1
        ), "no_of_sql_failed_while_validating does not match expected value"
        assert stat["error_log_file"], "error_log_file is None or empty"
        assert Path(stat["error_log_file"]).name.startswith("err_") and Path(stat["error_log_file"]).name.endswith(
            ".lst"
        ), "error_log_file does not match expected pattern 'err_*.lst'"

    expected_file_name = f"{input_dir}/query3.sql"
    expected_exception = f"Unsupported operation found in file {input_dir}/query3.sql."
    pattern = r"ValidationError\(file_name='(?P<file_name>[^']+)', exception='(?P<exception>[^']+)'\)"

    with open(Path(status[0]["error_log_file"])) as file:
        for line in file:
            # Skip empty lines
            if line.strip() == "":
                continue

            match = re.match(pattern, line)

            if match:
                # Extract information using group names from the pattern
                error_info = match.groupdict()
                # Perform assertions
                assert error_info["file_name"] == expected_file_name, "File name does not match the expected value"
                assert expected_exception in error_info["exception"], "Exception does not match the expected value"
            else:
                print("No match found.")

    # cleanup
    safe_remove_dir(input_dir)
    safe_remove_file(Path(status[0]["error_log_file"]))
>>>>>>> 96c6764d (Added Translation Support for `!` as `commands` and `&` for `Parameters` (#771))


def test_with_file(input_source, error_file, mock_workspace_client):
    sdk_config = create_autospec(Config)
    spark = create_autospec(DatabricksSession)
    config = TranspileConfig(
        transpiler_config_path="sqlglot",
        input_source=str(input_source / "query1.sql"),
        output_folder=None,
        error_file_path=str(error_file),
        sdk_config=sdk_config,
        source_dialect="snowflake",
        skip_validation=False,
    )
    mock_validate = create_autospec(Validator)
    mock_validate.spark = spark
    mock_validate.validate_format_result.return_value = ValidationResult(
        """ Mock validated query """, "Mock validation error"
    )

    with (
        patch(
            'databricks.labs.remorph.helpers.db_sql.get_sql_backend',
            return_value=MockBackend(),
        ),
        patch("databricks.labs.remorph.transpiler.execute.Validator", return_value=mock_validate),
    ):
        status, _errors = transpile(mock_workspace_client, SqlglotEngine(), config)

    # check the status
    check_status(status, 1, 1, 0, 0, 1, 0, error_file.name)
    # check errors
    expected_errors = [{"path": f"{input_source!s}/query1.sql", "message": "Mock validation error"}]
    check_error_lines(status["error_log_file"], expected_errors)


def test_with_file_with_output_folder_skip_validation(input_source, output_folder, mock_workspace_client):
    config = TranspileConfig(
        transpiler_config_path="sqlglot",
        input_source=str(input_source / "query1.sql"),
        output_folder=str(output_folder),
        sdk_config=None,
        source_dialect="snowflake",
        skip_validation=True,
    )

    with patch(
        'databricks.labs.remorph.helpers.db_sql.get_sql_backend',
        return_value=MockBackend(),
    ):
        status, _errors = transpile(mock_workspace_client, SqlglotEngine(), config)

    # check the status
    check_status(status, 1, 1, 0, 0, 0, 0, "None")


def test_with_not_a_sql_file_skip_validation(input_source, mock_workspace_client):
    config = TranspileConfig(
        transpiler_config_path="sqlglot",
        input_source=str(input_source / "file.txt"),
        output_folder=None,
        sdk_config=None,
        source_dialect="snowflake",
        skip_validation=True,
    )

    with patch(
        'databricks.labs.remorph.helpers.db_sql.get_sql_backend',
        return_value=MockBackend(),
    ):
        status, _errors = transpile(mock_workspace_client, SqlglotEngine(), config)

    # check the status
    check_status(status, 0, 0, 0, 0, 0, 0, "None")


def test_with_not_existing_file_skip_validation(input_source, mock_workspace_client):
    config = TranspileConfig(
        transpiler_config_path="sqlglot",
        input_source=str(input_source / "file_not_exist.txt"),
        output_folder=None,
        sdk_config=None,
        source_dialect="snowflake",
        skip_validation=True,
    )
    with pytest.raises(FileNotFoundError):
        with patch(
            'databricks.labs.remorph.helpers.db_sql.get_sql_backend',
            return_value=MockBackend(),
        ):
            transpile(mock_workspace_client, SqlglotEngine(), config)


def test_transpile_sql(mock_workspace_client):
    config = TranspileConfig(
        transpiler_config_path="sqlglot",
        source_dialect="snowflake",
        skip_validation=False,
        catalog_name="catalog",
        schema_name="schema",
    )
    query = """select col from table;"""

    with patch(
        'databricks.labs.remorph.helpers.db_sql.get_sql_backend',
        return_value=MockBackend(
            rows={
                "EXPLAIN SELECT": [Row(plan="== Physical Plan ==")],
            }
        ),
    ):
        transpiler_result, validation_result = transpile_sql(mock_workspace_client, config, query)
        assert transpiler_result.transpiled_code == 'SELECT\n  col\nFROM table'
        assert validation_result.exception_msg is None


def test_transpile_column_exp(mock_workspace_client):
    config = TranspileConfig(
        transpiler_config_path="sqlglot",
        source_dialect="snowflake",
        skip_validation=True,
        catalog_name="catalog",
        schema_name="schema",
    )
    query = ["case when col1 is null then 1 else 0 end", "col2 * 2", "current_timestamp()"]

    with patch(
        'databricks.labs.remorph.helpers.db_sql.get_sql_backend',
        return_value=MockBackend(
            rows={
                "EXPLAIN SELECT": [Row(plan="== Physical Plan ==")],
            }
        ),
    ):
        result = transpile_column_exp(mock_workspace_client, config, query)
        assert len(result) == 3
        assert result[0][0].transpiled_code == 'CASE WHEN col1 IS NULL THEN 1 ELSE 0 END'
        assert result[1][0].transpiled_code == 'col2 * 2'
        assert result[2][0].transpiled_code == 'CURRENT_TIMESTAMP()'
        assert result[0][0].error_list == []
        assert result[1][0].error_list == []
        assert result[2][0].error_list == []
        assert result[0][1] is None
        assert result[1][1] is None
        assert result[2][1] is None


def test_with_file_with_success(input_source, mock_workspace_client):
    sdk_config = create_autospec(Config)
    spark = create_autospec(DatabricksSession)
    config = TranspileConfig(
        transpiler_config_path="sqlglot",
        input_source=str(input_source / "query1.sql"),
        output_folder=None,
        sdk_config=sdk_config,
        source_dialect="snowflake",
        skip_validation=False,
    )
    mock_validate = create_autospec(Validator)
    mock_validate.spark = spark
    mock_validate.validate_format_result.return_value = ValidationResult(""" Mock validated query """, None)

    with (
        patch(
            'databricks.labs.remorph.helpers.db_sql.get_sql_backend',
            return_value=MockBackend(),
        ),
        patch("databricks.labs.remorph.transpiler.execute.Validator", return_value=mock_validate),
    ):
        status, _errors = transpile(mock_workspace_client, SqlglotEngine(), config)
        # assert the status
        check_status(status, 1, 1, 0, 0, 0, 0, "None")


def test_with_input_source_none(mock_workspace_client):
    config = TranspileConfig(
        transpiler_config_path="sqlglot",
        input_source=None,
        output_folder=None,
        sdk_config=None,
        source_dialect="snowflake",
        skip_validation=True,
    )

    with pytest.raises(ValueError, match="Input SQL path is not provided"):
        transpile(mock_workspace_client, SqlglotEngine(), config)


def test_parse_error_handling(input_source, error_file, mock_workspace_client):
    config = TranspileConfig(
        transpiler_config_path="sqlglot",
        input_source=str(input_source / "query4.sql"),
        output_folder=None,
        error_file_path=str(error_file),
        sdk_config=None,
        source_dialect="snowflake",
        skip_validation=True,
    )

    with patch('databricks.labs.remorph.helpers.db_sql.get_sql_backend', return_value=MockBackend()):
        status, _errors = transpile(mock_workspace_client, SqlglotEngine(), config)

    # assert the status
    check_status(status, 1, 1, 0, 1, 0, 0, error_file.name)
    # check errors
    expected_errors = [{"path": f"{input_source}/query4.sql", "message": "Parsing error Start:"}]
    check_error_lines(status["error_log_file"], expected_errors)


def test_token_error_handling(input_source, error_file, mock_workspace_client):
    config = TranspileConfig(
        transpiler_config_path="sqlglot",
        input_source=str(input_source / "query5.sql"),
        output_folder=None,
        error_file_path=str(error_file),
        sdk_config=None,
        source_dialect="snowflake",
        skip_validation=True,
    )

    with patch('databricks.labs.remorph.helpers.db_sql.get_sql_backend', return_value=MockBackend()):
        status, _errors = transpile(mock_workspace_client, SqlglotEngine(), config)
    # assert the status
    check_status(status, 1, 1, 0, 1, 0, 0, error_file.name)
    # check errors
    expected_errors = [{"path": f"{input_source}/query5.sql", "message": "Token error Start:"}]
    check_error_lines(status["error_log_file"], expected_errors)
