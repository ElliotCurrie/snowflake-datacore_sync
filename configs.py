import os


datacore_config = {
    "driver": os.getenv("DATACORE_DRIVER", "ODBC Driver 18 for SQL Server"),
    "server": os.environ["DATACORE_SERVER"],
    "database": os.environ["DATACORE_DATABASE"],
    "uid": os.environ["DATACORE_UID"],
    "password": os.environ["DATACORE_PASSWORD"],
    "encrypt": os.getenv("DATACORE_ENCRYPT", "yes"),
    "trust_server_certificate": os.getenv("DATACORE_TRUST_SERVER_CERTIFICATE", "yes"),
    "connection_timeout": int(os.getenv("DATACORE_CONNECTION_TIMEOUT", "30")),
}

core_snowflake_config = {
    "user": os.environ["CORE_SNOWFLAKE_USER"],
    "password": os.environ["CORE_SNOWFLAKE_PASSWORD"],
    "account": os.environ["CORE_SNOWFLAKE_ACCOUNT"],
    "warehouse": os.getenv("CORE_SNOWFLAKE_WAREHOUSE", "STANDARD_WH"),
    "database": os.getenv("CORE_SNOWFLAKE_DATABASE", "REAPIT_RAW"),
    "schema": os.getenv("CORE_SNOWFLAKE_SCHEMA", "AURORA_ROM_RPS_ROM"),
    "login_timeout": int(os.getenv("CORE_SNOWFLAKE_LOGIN_TIMEOUT", "30")),
    "network_timeout": int(os.getenv("CORE_SNOWFLAKE_NETWORK_TIMEOUT", "60")),
}

sa_snowflake_config = {
    "user": os.environ["SA_SNOWFLAKE_USER"],
    "password": os.environ["SA_SNOWFLAKE_PASSWORD"],
    "account": os.environ["SA_SNOWFLAKE_ACCOUNT"],
    "warehouse": os.getenv("SA_SNOWFLAKE_WAREHOUSE", "STANDARD_WH"),
    "database": os.getenv("SA_SNOWFLAKE_DATABASE", "REAPIT_RAW"),
    "schema": os.getenv("SA_SNOWFLAKE_SCHEMA", "AURORA_TWN_RPS_TWN"),
    "login_timeout": int(os.getenv("SA_SNOWFLAKE_LOGIN_TIMEOUT", "30")),
    "network_timeout": int(os.getenv("SA_SNOWFLAKE_NETWORK_TIMEOUT", "60")),
}
