from sqlalchemy import create_engine
from sqlalchemy.engine import URL
from sqlalchemy.orm import sessionmaker


def get_sqlpool_reader(
    config: dict,
    db_name: str,
    *,
    endpoint_key: str = 'dedicated_sql_endpoint',
    auth_type: str = 'sql_authentication',
):
    """
    :param auth_type:
    :param endpoint_key:
    :param config:
    :param db_name:
    :return: returns a sqlachemy reader for the given dedicated SQL Pool database
    """

    query_params = {
        "driver": config['driver'],
        "loginTimeout": "30",
    }

    if auth_type == "ad_passwd_authentication":
        query_params = {
            **query_params,
            "authentication": "ActiveDirectoryPassword",
        }
    elif auth_type == "spn_authentication":
        raise NotImplementedError("SPN Authentication not implemented yet")

    connection_string = URL.create(
        drivername="mssql+pyodbc",
        username=config['sql_user'],
        password=config['sql_password'],
        host=config[endpoint_key],
        port=config.get('port', 1433),
        database=db_name,
        query=query_params,
    )
    engine = create_engine(connection_string)
    session = sessionmaker(bind=engine)
    connection = session()

    return connection
