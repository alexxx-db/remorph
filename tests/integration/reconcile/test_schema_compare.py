import pytest

from databricks.labs.lakebridge.transpiler.sqlglot.dialect_utils import get_dialect
from databricks.labs.lakebridge.reconcile.recon_config import ColumnMapping, Table
from databricks.labs.lakebridge.reconcile.schema_compare import SchemaCompare

from tests.conftest import schema_fixture_factory


def snowflake_databricks_schema():
    src_schema = [
        schema_fixture_factory("col_boolean", "boolean"),
        schema_fixture_factory("col_char", "varchar(1)"),
        schema_fixture_factory("col_varchar", "varchar(16777216)"),
        schema_fixture_factory("col_string", "varchar(16777216)"),
        schema_fixture_factory("col_text", "varchar(16777216)"),
        schema_fixture_factory("col_binary", "binary(8388608)"),
        schema_fixture_factory("col_varbinary", "binary(8388608)"),
        schema_fixture_factory("col_int", "number(38,0)"),
        schema_fixture_factory("col_bigint", "number(38,0)"),
        schema_fixture_factory("col_smallint", "number(38,0)"),
        schema_fixture_factory("col_float", "float"),
        schema_fixture_factory("col_float4", "float"),
        schema_fixture_factory("col_double", "float"),
        schema_fixture_factory("col_real", "float"),
        schema_fixture_factory("col_date", "date"),
        schema_fixture_factory("col_time", "time(9)"),
        schema_fixture_factory("col_timestamp", "timestamp_ntz(9)"),
        schema_fixture_factory("col_timestamp_ltz", "timestamp_ltz(9)"),
        schema_fixture_factory("col_timestamp_ntz", "timestamp_ntz(9)"),
        schema_fixture_factory("col_timestamp_tz", "timestamp_tz(9)"),
        schema_fixture_factory("col_variant", "variant"),
        schema_fixture_factory("col_object", "object"),
        schema_fixture_factory("col_array", "array"),
        schema_fixture_factory("col_geography", "geography"),
        schema_fixture_factory("col_num10", "number(10,1)"),
        schema_fixture_factory("col_dec", "number(20,2)"),
        schema_fixture_factory("col_numeric_2", "numeric(38,0)"),
        schema_fixture_factory("col_escaped", "float", source_delimiter='"'),
        schema_fixture_factory("`col Escaped2`", "float", source_delimiter='"'),
        schema_fixture_factory('"col escaped3"', "float", source_delimiter='"'),
        schema_fixture_factory('"col""escaped4"', "float", source_delimiter='"'),
        schema_fixture_factory('"col`escaped5"', "float", source_delimiter='"'),
        schema_fixture_factory('"col `$ EscAped6"', "float", source_delimiter='"'),
        schema_fixture_factory("dummy", "string"),
    ]
    tgt_schema = [
        schema_fixture_factory("col_boolean", "boolean"),
        schema_fixture_factory("char", "string"),
        schema_fixture_factory("col_varchar", "string"),
        schema_fixture_factory("col_string", "string"),
        schema_fixture_factory("col_text", "string"),
        schema_fixture_factory("col_binary", "binary"),
        schema_fixture_factory("col_varbinary", "binary"),
        schema_fixture_factory("col_int", "decimal(38,0)"),
        schema_fixture_factory("col_bigint", "decimal(38,0)"),
        schema_fixture_factory("col_smallint", "decimal(38,0)"),
        schema_fixture_factory("col_float", "double"),
        schema_fixture_factory("col_float4", "double"),
        schema_fixture_factory("col_double", "double"),
        schema_fixture_factory("col_real", "double"),
        schema_fixture_factory("col_date", "date"),
        schema_fixture_factory("col_time", "timestamp"),
        schema_fixture_factory("col_timestamp", "timestamp_ntz"),
        schema_fixture_factory("col_timestamp_ltz", "timestamp"),
        schema_fixture_factory("col_timestamp_ntz", "timestamp_ntz"),
        schema_fixture_factory("col_timestamp_tz", "timestamp"),
        schema_fixture_factory("col_variant", "variant"),
        schema_fixture_factory("col_object", "string"),
        schema_fixture_factory("array_col", "array<string>"),
        schema_fixture_factory("col_geography", "string"),
        schema_fixture_factory("col_num10", "decimal(10,1)"),
        schema_fixture_factory("col_dec", "decimal(20,1)"),
        schema_fixture_factory("col_numeric_2", "decimal(38,0)"),
        schema_fixture_factory("col_escaped", "double", source_delimiter='`'),
        schema_fixture_factory("`col Escaped2`", "double", source_delimiter='`'),
        schema_fixture_factory('`col escaped3`', "double", source_delimiter='`'),
        schema_fixture_factory('`col"escaped4`', "double", source_delimiter='`'),
        schema_fixture_factory('`col``escaped5`', "double", source_delimiter='`'),
        schema_fixture_factory('`col ``$ EscAped6`', "double", source_delimiter='`'),
    ]
    return src_schema, tgt_schema


def databricks_databricks_schema():
    src_schema = [
        schema_fixture_factory("col_boolean", "boolean"),
        schema_fixture_factory("col_char", "string"),
        schema_fixture_factory("col_int", "int"),
        schema_fixture_factory("col_string", "string"),
        schema_fixture_factory("col_bigint", "int"),
        schema_fixture_factory("col_num10", "decimal(10,1)"),
        schema_fixture_factory("col_dec", "decimal(20,2)"),
        schema_fixture_factory("col_numeric_2", "decimal(38,0)"),
        schema_fixture_factory("col_escaped", "double", source_delimiter='`'),
        schema_fixture_factory("`col Escaped2`", "double", source_delimiter='`'),
        schema_fixture_factory('`col escaped3`', "double", source_delimiter='`'),
        schema_fixture_factory('`col"escaped4`', "double", source_delimiter='`'),
        schema_fixture_factory('`col``escaped5`', "double", source_delimiter='`'),
        schema_fixture_factory('`col ``$ EscAped6`', "double", source_delimiter='`'),
        schema_fixture_factory("dummy", "string"),
    ]
    tgt_schema = [
        schema_fixture_factory("col_boolean", "boolean"),
        schema_fixture_factory("char", "string"),
        schema_fixture_factory("col_int", "int"),
        schema_fixture_factory("col_string", "string"),
        schema_fixture_factory("col_bigint", "int"),
        schema_fixture_factory("col_num10", "decimal(10,1)"),
        schema_fixture_factory("col_dec", "decimal(20,1)"),
        schema_fixture_factory("col_numeric_2", "decimal(38,0)"),
        schema_fixture_factory("col_escaped", "double", source_delimiter='`'),
        schema_fixture_factory("`col Escaped2`", "double", source_delimiter='`'),
        schema_fixture_factory('`col escaped3`', "double", source_delimiter='`'),
        schema_fixture_factory('`col"escaped4`', "double", source_delimiter='`'),
        schema_fixture_factory('`col``escaped5`', "double", source_delimiter='`'),
        schema_fixture_factory('`col ``$ EscAped6`', "double", source_delimiter='`'),
    ]
    return src_schema, tgt_schema


def oracle_databricks_schema():
    src_schema = [
        schema_fixture_factory("col_xmltype", "xmltype"),
        schema_fixture_factory("col_char", "char(1)"),
        schema_fixture_factory("col_nchar", "nchar(255)"),
        schema_fixture_factory("col_varchar", "varchar2(255)"),
        schema_fixture_factory("col_varchar2", "varchar2(255)"),
        schema_fixture_factory("col_nvarchar", "nvarchar2(255)"),
        schema_fixture_factory("col_nvarchar2", "nvarchar2(255)"),
        schema_fixture_factory("col_character", "char(255)"),
        schema_fixture_factory("col_clob", "clob"),
        schema_fixture_factory("col_nclob", "nclob"),
        schema_fixture_factory("col_long", "long"),
        schema_fixture_factory("col_number", "number(10,2)"),
        schema_fixture_factory("col_float", "float"),
        schema_fixture_factory("col_binary_float", "binary_float"),
        schema_fixture_factory("col_binary_double", "binary_double"),
        schema_fixture_factory("col_date", "date"),
        schema_fixture_factory("col_timestamp", "timestamp(6)"),
        schema_fixture_factory("col_time_with_tz", "timestamp(6) with time zone"),
        schema_fixture_factory("col_timestamp_with_tz", "timestamp(6) with time zone"),
        schema_fixture_factory("col_timestamp_with_local_tz", "timestamp(6) with local time zone"),
        schema_fixture_factory("col_blob", "blob"),
        schema_fixture_factory("col_rowid", "rowid"),
        schema_fixture_factory("col_urowid", "urowid"),
        schema_fixture_factory("col_anytype", "anytype"),
        schema_fixture_factory("col_anydata", "anydata"),
        schema_fixture_factory("col_anydataset", "anydataset"),
        schema_fixture_factory("col_escaped", "float", source_delimiter='"'),
        schema_fixture_factory("`col Escaped2`", "float", source_delimiter='"'),
        schema_fixture_factory('"col escaped3"', "float", source_delimiter='"'),
        schema_fixture_factory('"col""escaped4"', "float", source_delimiter='"'),
        schema_fixture_factory('"col`escaped5"', "float", source_delimiter='"'),
        schema_fixture_factory('"col `$ EscAped6"', "float", source_delimiter='"'),
        schema_fixture_factory("dummy", "string"),
    ]

    tgt_schema = [
        schema_fixture_factory("col_xmltype", "string"),
        schema_fixture_factory("char", "string"),
        schema_fixture_factory("col_nchar", "string"),
        schema_fixture_factory("col_varchar", "string"),
        schema_fixture_factory("col_varchar2", "string"),
        schema_fixture_factory("col_nvarchar", "string"),
        schema_fixture_factory("col_nvarchar2", "string"),
        schema_fixture_factory("col_character", "string"),
        schema_fixture_factory("col_clob", "string"),
        schema_fixture_factory("col_nclob", "string"),
        schema_fixture_factory("col_long", "string"),
        schema_fixture_factory("col_number", "DECIMAL(10,2)"),
        schema_fixture_factory("col_float", "double"),
        schema_fixture_factory("col_binary_float", "double"),
        schema_fixture_factory("col_binary_double", "double"),
        schema_fixture_factory("col_date", "date"),
        schema_fixture_factory("col_timestamp", "timestamp"),
        schema_fixture_factory("col_time_with_tz", "timestamp"),
        schema_fixture_factory("col_timestamp_with_tz", "timestamp"),
        schema_fixture_factory("col_timestamp_with_local_tz", "timestamp"),
        schema_fixture_factory("col_blob", "binary"),
        schema_fixture_factory("col_rowid", "string"),
        schema_fixture_factory("col_urowid", "string"),
        schema_fixture_factory("col_anytype", "string"),
        schema_fixture_factory("col_anydata", "string"),
        schema_fixture_factory("col_anydataset", "string"),
        schema_fixture_factory("col_escaped", "double", source_delimiter='`'),
        schema_fixture_factory("`col Escaped2`", "double", source_delimiter='`'),
        schema_fixture_factory('`col escaped3`', "double", source_delimiter='`'),
        schema_fixture_factory('`col"escaped4`', "double", source_delimiter='`'),
        schema_fixture_factory('`col``escaped5`', "double", source_delimiter='`'),
        schema_fixture_factory('`col ``$ EscAped6`', "double", source_delimiter='`'),
    ]

    return src_schema, tgt_schema


@pytest.fixture
def schemas():
    return {
        "snowflake_databricks_schema": snowflake_databricks_schema(),
        "databricks_databricks_schema": databricks_databricks_schema(),
        "oracle_databricks_schema": oracle_databricks_schema(),
    }


def test_snowflake_schema_compare(schemas, mock_spark):
    src_schema, tgt_schema = schemas["snowflake_databricks_schema"]
    spark = mock_spark
    table_conf = Table(
        source_name="supplier",
        target_name="supplier",
        drop_columns=["dummy"],
        column_mapping=[
            ColumnMapping(source_name="col_char", target_name="char"),
            ColumnMapping(source_name="col_array", target_name="array_col"),
        ],
    )

    schema_compare_output = SchemaCompare(spark).compare(
        src_schema,
        tgt_schema,
        get_dialect("snowflake"),
        table_conf,
    )
    df = schema_compare_output.compare_df
    assert not schema_compare_output.is_valid
    assert df.count() == 33
    assert df.filter("is_valid = 'true'").count() == 31
    assert df.filter("is_valid = 'false'").count() == 2


def test_databricks_schema_compare(schemas, mock_spark):
    src_schema, tgt_schema = schemas["databricks_databricks_schema"]
    spark = mock_spark
    table_conf = Table(
        source_name="supplier",
        target_name="supplier",
        select_columns=[
            "col_boolean",
            "col_char",
            "col_int",
            "col_string",
            "col_bigint",
            "col_num10",
            "col_dec",
            "col_numeric_2",
            "`col_escaped`",
            "`col Escaped2`",
            '`col escaped3`',
            '`col"escaped4`',
            '`col``escaped5`',
            '`col ``$ EscAped6`',
        ],
        column_mapping=[
            ColumnMapping(source_name="col_char", target_name="char"),
            ColumnMapping(source_name="col_array", target_name="array_col"),
        ],
    )
    schema_compare_output = SchemaCompare(spark).compare(
        src_schema,
        tgt_schema,
        get_dialect("databricks"),
        table_conf,
    )
    df = schema_compare_output.compare_df

    assert not schema_compare_output.is_valid
    assert df.count() == 14
    assert df.filter("is_valid = 'true'").count() == 13
    assert df.filter("is_valid = 'false'").count() == 1


def test_oracle_schema_compare(schemas, mock_spark):
    src_schema, tgt_schema = schemas["oracle_databricks_schema"]
    spark = mock_spark
    table_conf = Table(
        source_name="supplier",
        target_name="supplier",
        drop_columns=["dummy"],
        column_mapping=[
            ColumnMapping(source_name="col_char", target_name="char"),
            ColumnMapping(source_name="col_array", target_name="array_col"),
        ],
    )
    schema_compare_output = SchemaCompare(spark).compare(
        src_schema,
        tgt_schema,
        get_dialect("oracle"),
        table_conf,
    )
    df = schema_compare_output.compare_df

    assert schema_compare_output.is_valid
    assert df.count() == 32
    assert df.filter("is_valid = 'true'").count() == 32
    assert df.filter("is_valid = 'false'").count() == 0


def test_schema_compare(mock_spark):
    src_schema = [
        schema_fixture_factory("col1", "int", "`col1`", "`col1`"),
        schema_fixture_factory("col2", "string", "`col2`", "`col2`"),
    ]
    tgt_schema = [
        schema_fixture_factory("col1", "int", "`col1`", "`col1`"),
        schema_fixture_factory("col2", "string", "`col2`", "`col2`"),
    ]
    spark = mock_spark
    table_conf = Table(
        source_name="supplier",
        target_name="supplier",
        drop_columns=["dummy"],
        column_mapping=[
            ColumnMapping(source_name="col_char", target_name="char"),
            ColumnMapping(source_name="col_array", target_name="array_col"),
        ],
    )

    schema_compare_output = SchemaCompare(spark).compare(
        src_schema,
        tgt_schema,
        get_dialect("databricks"),
        table_conf,
    )
    df = schema_compare_output.compare_df

    assert schema_compare_output.is_valid
    assert df.count() == 2
    assert df.filter("is_valid = 'true'").count() == 2
    assert df.filter("is_valid = 'false'").count() == 0


def test_schema_compare_large_column_count_bug_validation(mock_spark):
    """
    Test to validate the bug in issue #1973 where schema comparison
    dashboard contains only 50 rows even when table has more than 50 columns.
    This test creates a schema with more than 50 columns to reproduce the bug.
    
    The bug is likely in the data persistence layer where collect_list might 
    have a default limit or the explode operation in the dashboard query
    might be limited.
    """
    # Create 60 columns to test the 50-row limit bug
    src_schema = []
    tgt_schema = []
    
    for i in range(1, 61):  # 60 columns
        col_name = f"col_{i:03d}"
        src_schema.append(schema_fixture_factory(col_name, "string", f"`{col_name}`", f"`{col_name}`"))
        tgt_schema.append(schema_fixture_factory(col_name, "string", f"`{col_name}`", f"`{col_name}`"))
    
    spark = mock_spark
    table_conf = Table(
        source_name="large_table",
        target_name="large_table",
        drop_columns=[],
        column_mapping=[],
    )

    schema_compare_output = SchemaCompare(spark).compare(
        src_schema,
        tgt_schema,
        get_dialect("databricks"),
        table_conf,
    )
    df = schema_compare_output.compare_df

    # The bug is that we expect 60 rows but only get 50
    # If the bug exists, this assertion will fail
    actual_count = df.count()
    valid_count = df.filter("is_valid = 'true'").count()
    invalid_count = df.filter("is_valid = 'false'").count()
    
    # Debug information for troubleshooting
    print(f"DEBUG: Created {len(src_schema)} source columns and {len(tgt_schema)} target columns")
    print(f"DEBUG: Schema comparison returned {actual_count} rows")
    print(f"DEBUG: Valid rows: {valid_count}, Invalid rows: {invalid_count}")
    
    assert actual_count == 60, (
        f"BUG CONFIRMED (Issue #1973): Expected 60 rows in schema comparison result, "
        f"but got {actual_count}. This confirms the 50-row limit bug exists. "
        f"Schema comparison processed {len(src_schema)} columns but only returned {actual_count} rows."
    )
    assert valid_count == 60
    assert invalid_count == 0
    assert schema_compare_output.is_valid


def test_schema_compare_exactly_50_columns(mock_spark):
    """
    Test with exactly 50 columns to see if this works fine.
    This helps isolate whether the issue is specifically with >50 columns.
    """
    # Create exactly 50 columns
    src_schema = []
    tgt_schema = []
    
    for i in range(1, 51):  # 50 columns
        col_name = f"col_{i:03d}"
        src_schema.append(schema_fixture_factory(col_name, "string", f"`{col_name}`", f"`{col_name}`"))
        tgt_schema.append(schema_fixture_factory(col_name, "string", f"`{col_name}`", f"`{col_name}`"))
    
    spark = mock_spark
    table_conf = Table(
        source_name="fifty_col_table",
        target_name="fifty_col_table",
        drop_columns=[],
        column_mapping=[],
    )

    schema_compare_output = SchemaCompare(spark).compare(
        src_schema,
        tgt_schema,
        get_dialect("databricks"),
        table_conf,
    )
    df = schema_compare_output.compare_df

    # This should work fine with exactly 50 columns
    assert df.count() == 50, f"Expected 50 rows in schema comparison result, but got {df.count()}"
    assert df.filter("is_valid = 'true'").count() == 50
    assert df.filter("is_valid = 'false'").count() == 0
    assert schema_compare_output.is_valid


def test_schema_compare_51_columns_edge_case(mock_spark):
    """
    Test with exactly 51 columns to see if the issue starts at >50.
    This helps pinpoint the exact threshold where the bug occurs.
    """
    # Create 51 columns to test the edge case
    src_schema = []
    tgt_schema = []
    
    for i in range(1, 52):  # 51 columns
        col_name = f"col_{i:03d}"
        src_schema.append(schema_fixture_factory(col_name, "string", f"`{col_name}`", f"`{col_name}`"))
        tgt_schema.append(schema_fixture_factory(col_name, "string", f"`{col_name}`", f"`{col_name}`"))
    
    spark = mock_spark
    table_conf = Table(
        source_name="fifty_one_col_table",
        target_name="fifty_one_col_table",
        drop_columns=[],
        column_mapping=[],
    )

    schema_compare_output = SchemaCompare(spark).compare(
        src_schema,
        tgt_schema,
        get_dialect("databricks"),
        table_conf,
    )
    df = schema_compare_output.compare_df

    # If the bug exists, this might return only 50 rows instead of 51
    actual_count = df.count()
    valid_count = df.filter("is_valid = 'true'").count()
    invalid_count = df.filter("is_valid = 'false'").count()
    
    print(f"DEBUG: Edge case test - Created {len(src_schema)} columns, got {actual_count} rows")
    
    if actual_count == 50:
        # Bug confirmed: 51 columns but only 50 rows returned
        assert False, (
            f"BUG DETECTED (Issue #1973): Expected 51 rows but got {actual_count}. "
            f"The 50-row limit bug is confirmed. Schema had {len(src_schema)} columns "
            f"but comparison returned only {actual_count} rows."
        )
    else:
        # No bug: all 51 rows returned as expected
        assert actual_count == 51, f"Expected 51 rows in schema comparison result, but got {actual_count}"
        assert valid_count == 51
        assert invalid_count == 0
        assert schema_compare_output.is_valid
