"""
Lightweight database client wrappers used by the sync engine.

These classes deliberately expose a small common surface area: connect,
reconnect, availability checks, cursor access, and a run_when_available wrapper.
That keeps the sync code mostly database-agnostic while still allowing each
backend to handle its own connection details.
"""

import pyodbc
import snowflake.connector
import time


class SqlServerClient:
    """Small wrapper around a pyodbc SQL Server connection."""
    def __init__(
        self,
        driver,
        server,
        database,
        uid,
        password,
        encrypt="yes",
        trust_server_certificate="yes",
        connection_timeout=30,
    ):
        self.driver = driver
        self.server = server
        self.database = database
        self.uid = uid
        self.password = password
        self.encrypt = encrypt
        self.trust_server_certificate = trust_server_certificate
        self.connection_timeout = connection_timeout

        self.conn = None
        self.connect()

    def connect(self):
        """Open a SQL Server connection using the configured ODBC driver."""
        self.conn = pyodbc.connect(
            f"DRIVER={{{self.driver}}};"
            f"SERVER={self.server};"
            f"DATABASE={self.database};"
            f"UID={self.uid};"
            f"PWD={self.password};"
            f"Encrypt={self.encrypt};"
            f"TrustServerCertificate={self.trust_server_certificate};"
            f"Connection Timeout={self.connection_timeout};"
        )

        self.conn.autocommit = False

    def reconnect(self):
        """Close the current connection if possible, then open a new one."""
        try:
            self.conn.close()
        except Exception:
            pass

        self.connect()

    def is_available(self):
        """Return True when a trivial SQL statement succeeds."""
        try:
            cur = self.conn.cursor()
            cur.execute("SELECT 1;")
            cur.fetchone()
            cur.close()
            return True
        except Exception:
            return False

    def wait_until_available(self, check_interval_seconds=30):
        """Block until SQL Server accepts a new connection again."""
        while True:
            try:
                self.reconnect()

                if self.is_available():
                    return

            except Exception as e:
                print(f"SQL unavailable: {e}")

            print(f"Waiting {check_interval_seconds}s for SQL connection...")
            time.sleep(check_interval_seconds)

    def run_when_available(self, func, *args, **kwargs):
        """
        Run a database operation, retrying only when the connection has dropped.

        If SQL Server is still reachable after an exception, the exception is
        treated as a real query/data error and is raised immediately.
        """
        while True:
            try:
                return func(self, *args, **kwargs)

            except Exception as e:
                if self.is_available():
                    raise

                print(f"Connection lost during {func.__name__}: {e}", flush=True)
                self.wait_until_available()

    def cursor(self):
        return self.conn.cursor()

    def commit(self):
        self.conn.commit()

    def rollback(self):
        self.conn.rollback()

    def close(self):
        try:
            self.conn.close()
        except Exception:
            pass

    def list_tables(self, schema_name):
        """List user tables in a SQL Server schema."""
        sql = """
            SELECT
                t.name AS table_name
            FROM sys.tables AS t
            INNER JOIN sys.schemas AS s
                ON t.schema_id = s.schema_id
            WHERE s.name = ?
            ORDER BY t.name;
        """

        cur = self.cursor()

        try:
            cur.execute(sql, (schema_name,))

            cols = [c[0].lower() for c in cur.description]
            rows = cur.fetchall()

            results = []

            for row in rows:
                item = dict(zip(cols, row))
                item["table_name"] = item["table_name"].lower()
                results.append(item)

            return results

        finally:
            cur.close()

    def get_table_schema(self, schema_name, table_name):
        """Return SQL Server column metadata for a target or staging table."""
        sql = """
            SELECT
                column_name,
                data_type,
                ordinal_position,
                character_maximum_length,
                numeric_precision,
                numeric_scale,
                is_nullable
            FROM information_schema.columns
            WHERE
                table_schema = ?
                AND table_name = ?
            ORDER BY ordinal_position;
        """

        cur = self.cursor()

        try:
            cur.execute(
                sql,
                (
                    schema_name,
                    table_name,
                )
            )

            cols = [c[0].lower() for c in cur.description]
            rows = cur.fetchall()

            results = []

            for row in rows:
                item = dict(zip(cols, row))
                item["column_name"] = item["column_name"].lower()
                item["data_type"] = item["data_type"].upper()
                results.append(item)

            if not results:
                raise ValueError(
                    f"No columns found for SQL Server table: {schema_name}.{table_name}"
                )

            return results

        finally:
            cur.close()


class SnowflakeClient:
    """Small wrapper around a Snowflake connector connection."""
    def __init__(
        self,
        user,
        password,
        account,
        warehouse,
        database,
        schema,
        login_timeout=30,
        network_timeout=60,
    ):
        self.user = user
        self.password = password
        self.account = account
        self.warehouse = warehouse
        self.database = database
        self.schema = schema
        self.login_timeout = login_timeout
        self.network_timeout = network_timeout

        self.conn = None
        self.connect()

    def connect(self):
        """Open a Snowflake connection using the configured warehouse/database/schema."""
        self.conn = snowflake.connector.connect(
            user=self.user,
            password=self.password,
            account=self.account,
            warehouse=self.warehouse,
            database=self.database,
            schema=self.schema,
            login_timeout=self.login_timeout,
            network_timeout=self.network_timeout,
        )

    def reconnect(self):
        """Close the current Snowflake connection if possible, then reconnect."""
        try:
            self.conn.close()
        except Exception:
            pass

        self.connect()

    def is_available(self):
        """Return True when a trivial Snowflake statement succeeds."""
        try:
            cur = self.conn.cursor()
            cur.execute("SELECT 1;")
            cur.fetchone()
            cur.close()
            return True
        except Exception:
            return False

    def wait_until_available(self, check_interval_seconds=30):
        """Block until Snowflake accepts a new connection again."""
        while True:
            try:
                self.reconnect()

                if self.is_available():
                    return

            except Exception as e:
                print(f"Snowflake unavailable: {e}")

            print(f"Waiting {check_interval_seconds}s for Snowflake connection...")
            time.sleep(check_interval_seconds)

    def run_when_available(self, func, *args, **kwargs):
        """
        Run a Snowflake operation, retrying only when the connection has dropped.

        Query/data errors are not hidden. If Snowflake is reachable after the
        exception, the original exception is re-raised.
        """
        while True:
            try:
                return func(self, *args, **kwargs)

            except Exception as e:
                if self.is_available():
                    raise

                print(f"Connection lost during {func.__name__}: {e}", flush=True)
                self.wait_until_available()

    def cursor(self):
        return self.conn.cursor()
    
    def close(self):
        try:
            self.conn.close()
        except Exception:
            pass

    def list_tables(self):
        """List base tables in the configured Snowflake schema."""
        sql = """
            SELECT
                table_name
            FROM information_schema.tables
            WHERE table_schema = %s
            AND table_type = 'BASE TABLE'
            ORDER BY table_name
        """

        cur = self.cursor()

        try:
            cur.execute(sql, (self.schema.upper()))

            cols = [c[0].lower() for c in cur.description]
            rows = cur.fetchall()

            return [
                dict(zip(cols, row))
                for row in rows
            ]

        finally:
            cur.close()

    def get_table_schema(self, table_name):
        """
        Return Snowflake column metadata and mark primary-key columns.

        SHOW PRIMARY KEYS is used because information_schema.columns does not
        expose the primary-key flag in the shape needed by the sync engine.
        """
        table_name_upper = table_name.upper()

        primary_key_sql = (
            f"SHOW PRIMARY KEYS IN TABLE {table_name_upper}"
        )

        columns_sql = """
            SELECT
                column_name,
                data_type,
                ordinal_position,
                character_maximum_length,
                numeric_precision,
                numeric_scale,
                is_nullable
            FROM information_schema.columns
            WHERE table_schema = %s
            AND table_name = %s
            ORDER BY ordinal_position
        """

        cur = self.cursor()

        try:
            cur.execute(primary_key_sql)

            pk_cols = [c[0].lower() for c in cur.description]
            pk_rows = cur.fetchall()

            primary_key_columns = set()

            for row in pk_rows:
                item = dict(zip(pk_cols, row))
                primary_key_columns.add(item["column_name"].lower())

            cur.execute(
                columns_sql,
                (
                    self.schema.upper(),
                    table_name_upper,
                )
            )

            cols = [c[0].lower() for c in cur.description]
            rows = cur.fetchall()

            results = []

            for row in rows:
                item = dict(zip(cols, row))
                item["column_name"] = item["column_name"].lower()
                item["data_type"] = item["data_type"].upper()
                item["is_primary_key"] = (
                    item["column_name"] in primary_key_columns
                )
                results.append(item)

            if not results:
                raise ValueError(
                    f"No columns found for Snowflake table: {self.schema}.{table_name_upper}"
                )

            return results

        finally:
            cur.close()
