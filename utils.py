from datetime import datetime, date
from pathlib import Path

def log(message, max_length=2000):
    log_dir = Path("logs")
    now = datetime.now()

    message = str(message)

    if len(message) > max_length:
        message = (
            message[:max_length]
            + f"... [TRUNCATED - original length: {len(message)} chars]"
        )

    timestamped_message = f"[{now:%Y-%m-%d %H:%M:%S}] {message}"

    print(timestamped_message)

    log_dir.mkdir(parents=True, exist_ok=True)

    log_file = log_dir / f"snowflake_datacore_sync_{now:%Y_%m_%d}.log"

    with open(log_file, "a", encoding="utf-8") as f:
        f.write(timestamped_message + "\n")


def truncate_error(error: Exception | str | None, max_length: int = 2000) -> str | None:
    if error is None:
        return None

    error_text = str(error)

    if len(error_text) <= max_length:
        return error_text

    suffix = f"... [truncated, original length: {len(error_text)} chars]"
    keep_length = max_length - len(suffix)

    if keep_length <= 0:
        return error_text[:max_length]

    return error_text[:keep_length] + suffix


def snowflake_column_to_sql_server_definition(column, primary_key=None):
    column_name = column["column_name"].lower()
    data_type = column["data_type"].upper()

    is_primary_key = (
        primary_key is not None
        and column_name == primary_key.lower()
    )

    character_maximum_length = column.get("character_maximum_length")
    numeric_precision = column.get("numeric_precision")
    numeric_scale = column.get("numeric_scale")

    if data_type in {"TEXT", "VARCHAR", "CHAR", "CHARACTER", "STRING"}:
        if is_primary_key:
            sql_type = "NVARCHAR(450)"
        elif character_maximum_length:
            length = int(character_maximum_length)

            if 1 <= length <= 4000:
                sql_type = f"NVARCHAR({length})"
            else:
                sql_type = "NVARCHAR(MAX)"
        else:
            sql_type = "NVARCHAR(MAX)"

    elif data_type in {"NUMBER", "NUMERIC", "DECIMAL"}:
        if numeric_precision is not None:
            precision = min(int(numeric_precision), 38)
            scale = int(numeric_scale or 0)
            scale = min(scale, precision)

            sql_type = f"DECIMAL({precision}, {scale})"
        else:
            sql_type = "DECIMAL(38, 10)"

    elif data_type in {"INT", "INTEGER", "BIGINT"}:
        sql_type = "BIGINT"

    elif data_type == "SMALLINT":
        sql_type = "SMALLINT"

    elif data_type in {"TINYINT", "BYTEINT"}:
        sql_type = "TINYINT"

    elif data_type in {
        "FLOAT",
        "FLOAT4",
        "FLOAT8",
        "DOUBLE",
        "DOUBLE PRECISION",
        "REAL",
    }:
        sql_type = "FLOAT"

    elif data_type == "BOOLEAN":
        sql_type = "BIT"

    elif data_type == "DATE":
        sql_type = "DATE"

    elif data_type in {
        "TIMESTAMP",
        "TIMESTAMP_NTZ",
        "TIMESTAMP_LTZ",
        "TIMESTAMP_TZ",
        "DATETIME",
    }:
        sql_type = "DATETIME2"

    elif data_type == "TIME":
        sql_type = "TIME"

    elif data_type in {"VARIANT", "OBJECT", "ARRAY"}:
        sql_type = "NVARCHAR(MAX)"

    elif data_type in {"BINARY", "VARBINARY"}:
        sql_type = "VARBINARY(MAX)"

    else:
        sql_type = "NVARCHAR(MAX)"

    nullable = "NOT NULL" if is_primary_key else "NULL"

    escaped_column_name = column_name.replace("]", "]]")

    return f"[{escaped_column_name}] {sql_type} {nullable}"


def insert_db_schema_evo_log(
    datacore,
    schema_name,
    table_name,
):
    sql = """
        INSERT INTO ops.db_schema_evo_log (
            schema_name,
            table_name
        )
        VALUES (
            ?,
            ?
        );
    """

    cur = datacore.cursor()

    try:
        cur.execute(
            sql,
            (
                schema_name,
                table_name,
            ),
        )

        datacore.commit()

    except Exception:
        datacore.rollback()
        raise

    finally:
        cur.close()


def insert_tbl_schema_evo_log(
    datacore,
    schema_name,
    table_name,
    column_name,
):
    sql = """
        INSERT INTO ops.tbl_schema_evo_log (
            schema_name,
            table_name,
            column_name
        )
        VALUES (
            ?,
            ?,
            ?
        );
    """

    cur = datacore.cursor()

    try:
        cur.execute(
            sql,
            (
                schema_name,
                table_name,
                column_name,
            ),
        )

        datacore.commit()

    except Exception:
        datacore.rollback()
        raise

    finally:
        cur.close()


def handle_schema_changes(
    snowflake,
    datacore,
    sync_schema_name="reapit",
    staging_schema_name="stg",
):
    def normalise_name(name):
        return str(name).lower()

    def upsert_datacore_table_config(
        datacore,
        schema_name,
        table_name,
        primary_key,
    ):
        sql = """
            UPDATE ops.table_config
            SET
                primary_key = ?
            WHERE
                schema_name = ?
                AND table_name = ?;

            IF @@ROWCOUNT = 0
            BEGIN
                INSERT INTO ops.table_config (
                    schema_name,
                    table_name,
                    primary_key,
                    last_synced,
                    last_pk
                )
                VALUES (
                    ?,
                    ?,
                    ?,
                    NULL,
                    NULL
                );
            END;
        """

        cur = datacore.cursor()

        try:
            cur.execute(
                sql,
                (
                    primary_key,
                    schema_name,
                    table_name,
                    schema_name,
                    table_name,
                    primary_key,
                ),
            )

            datacore.commit()

        except Exception:
            datacore.rollback()
            raise

        finally:
            cur.close()

    def get_single_primary_key_from_schema(table_schema, table_name):
        primary_key_columns = [
            column["column_name"].lower()
            for column in table_schema
            if column.get("is_primary_key")
        ]

        if not primary_key_columns:
            raise ValueError(
                f"No primary key found for Snowflake table "
                f"{sync_schema_name}.{table_name}"
            )

        if len(primary_key_columns) > 1:
            raise ValueError(
                f"Composite primary key found for Snowflake table "
                f"{sync_schema_name}.{table_name}: {primary_key_columns}. "
                f"Current sync expects a single-column primary key."
            )

        return primary_key_columns[0]

    def create_target_table_from_snowflake_schema(
        datacore,
        schema_name,
        table_name,
        table_schema,
        primary_key,
    ):
        column_definitions = [
            snowflake_column_to_sql_server_definition(
                column,
                primary_key=primary_key,
            )
            for column in table_schema
        ]

        constraint_name = f"pk_{schema_name}_{table_name}"

        sql = f"""
            CREATE TABLE {schema_name}.{table_name} (
                {", ".join(column_definitions)},
                CONSTRAINT {constraint_name}
                    PRIMARY KEY ({primary_key})
            );
        """

        cur = datacore.cursor()

        try:
            cur.execute(sql)
            datacore.commit()

        except Exception:
            datacore.rollback()
            raise

        finally:
            cur.close()

    def create_staging_table_from_target(
        datacore,
        source_schema_name,
        table_name,
    ):
        staging_table_name = f"{source_schema_name}_{table_name}"

        sql = f"""
            SELECT TOP 0 *
            INTO {staging_schema_name}.{staging_table_name}
            FROM {source_schema_name}.{table_name};
        """

        cur = datacore.cursor()

        try:
            cur.execute(sql)
            datacore.commit()

        except Exception:
            datacore.rollback()
            raise

        finally:
            cur.close()

    # ------------------------------------------------------------------
    # 1. List target tables at both ends
    # ------------------------------------------------------------------

    sf_tables = snowflake.run_when_available(
        type(snowflake).list_tables,
    )

    datacore_target_tables = datacore.run_when_available(
        type(datacore).list_tables,
        schema_name=sync_schema_name,
    )

    sf_table_names = {
        normalise_name(row["table_name"])
        for row in sf_tables
    }

    existing_datacore_target_tables = {
        normalise_name(row["table_name"])
        for row in datacore_target_tables
    }

    # ------------------------------------------------------------------
    # 2. Find missing target tables
    # ------------------------------------------------------------------

    missing_target_tables = sorted(
        sf_table_names - existing_datacore_target_tables
    )

    log(f"Snowflake tables found: {len(sf_table_names)}")
    log(
        f"Existing Datacore {sync_schema_name} tables found: "
        f"{len(existing_datacore_target_tables)}"
    )
    log(f"Missing target tables found: {len(missing_target_tables)}")

    target_tables_added = []

    # ------------------------------------------------------------------
    # 3. Create missing target tables from Snowflake schema
    # ------------------------------------------------------------------

    for table_name in missing_target_tables:
        log(f"Creating missing target table: {sync_schema_name}.{table_name}")

        table_schema = snowflake.run_when_available(
            type(snowflake).get_table_schema,
            table_name=table_name,
        )

        primary_key = get_single_primary_key_from_schema(
            table_schema=table_schema,
            table_name=table_name,
        )

        create_target_table_from_snowflake_schema(
            datacore=datacore,
            schema_name=sync_schema_name,
            table_name=table_name,
            table_schema=table_schema,
            primary_key=primary_key,
        )

        insert_db_schema_evo_log(
            datacore=datacore,
            schema_name=sync_schema_name,
            table_name=table_name,
        )

        target_tables_added.append(
            {
                "schema_name": sync_schema_name,
                "table_name": table_name,
                "primary_key": primary_key,
            }
        )

        log(
            f"Created target table: "
            f"{sync_schema_name}.{table_name}, pk={primary_key}"
        )

    # ------------------------------------------------------------------
    # 4. Refresh target table list after creation
    # ------------------------------------------------------------------

    datacore_target_tables = datacore.run_when_available(
        type(datacore).list_tables,
        schema_name=sync_schema_name,
    )

    datacore_target_tables = {
        normalise_name(row["table_name"])
        for row in datacore_target_tables
    }

    # ------------------------------------------------------------------
    # 5. List staging tables in Datacore
    # ------------------------------------------------------------------

    datacore_staging_tables = datacore.run_when_available(
        type(datacore).list_tables,
        schema_name=staging_schema_name,
    )

    datacore_staging_tables = {
        normalise_name(row["table_name"])
        for row in datacore_staging_tables
    }

    # ------------------------------------------------------------------
    # 6. Find missing staging tables versus target tables
    # ------------------------------------------------------------------

    expected_staging_tables = {
        f"{sync_schema_name}_{table_name}"
        for table_name in datacore_target_tables
    }

    missing_staging_tables = sorted(
        expected_staging_tables - datacore_staging_tables
    )

    log(f"Expected staging tables: {len(expected_staging_tables)}")
    log(f"Existing staging tables: {len(datacore_staging_tables)}")
    log(f"Missing staging tables found: {len(missing_staging_tables)}")

    staging_tables_added = []

    # ------------------------------------------------------------------
    # 7. Create missing staging tables from target tables
    # ------------------------------------------------------------------

    for staging_table_name in missing_staging_tables:
        table_name = staging_table_name.removeprefix(f"{sync_schema_name}_")

        log(
            f"Creating missing staging table: "
            f"{staging_schema_name}.{staging_table_name} "
            f"from {sync_schema_name}.{table_name}"
        )

        create_staging_table_from_target(
            datacore=datacore,
            source_schema_name=sync_schema_name,
            table_name=table_name,
        )

        insert_db_schema_evo_log(
            datacore=datacore,
            schema_name=staging_schema_name,
            table_name=staging_table_name,
        )

        staging_tables_added.append(
            {
                "schema_name": staging_schema_name,
                "table_name": staging_table_name,
            }
        )

        log(f"Created staging table: {staging_schema_name}.{staging_table_name}")

    # ------------------------------------------------------------------
    # 8. Upsert ops.table_config for newly created target tables
    # ------------------------------------------------------------------

    for table in target_tables_added:
        upsert_datacore_table_config(
            datacore=datacore,
            schema_name=table["schema_name"],
            table_name=table["table_name"],
            primary_key=table["primary_key"],
        )

        log(
            f"Upserted ops.table_config: "
            f"{table['schema_name']}.{table['table_name']}, "
            f"pk={table['primary_key']}"
        )

    log("Schema change handling complete.")

    return {
        "snowflake_table_count": len(sf_table_names),
        "target_table_count": len(datacore_target_tables),
        "target_tables_added": target_tables_added,
        "staging_tables_added": staging_tables_added,
    }


def ensure_columns_exist_from_payload(
    datacore,
    schema_name,
    table_name,
    sf_columns,
    snowflake_schema,
):
    target_schema = datacore.run_when_available(
        type(datacore).get_table_schema,
        schema_name=schema_name,
        table_name=table_name,
    )

    existing_column_names = {
        row["column_name"].lower()
        for row in target_schema
    }

    missing_column_names = {
        col.lower()
        for col in sf_columns
    } - existing_column_names

    if not missing_column_names:
        return []

    missing_columns = [
        column
        for column in snowflake_schema
        if column["column_name"].lower() in missing_column_names
    ]

    added_columns = []

    for column in missing_columns:
        column_definition = snowflake_column_to_sql_server_definition(
            column,
            primary_key=None,
        )

        sql = f"""
            ALTER TABLE {schema_name}.{table_name}
            ADD {column_definition};
        """

        cur = datacore.cursor()

        try:
            cur.execute(sql)
            datacore.commit()

        except Exception:
            datacore.rollback()
            raise

        finally:
            cur.close()

        insert_tbl_schema_evo_log(
            datacore=datacore,
            schema_name=schema_name,
            table_name=table_name,
            column_name=column["column_name"],
        )

        added_columns.append(column["column_name"])

    return added_columns


def wait_for_all_connections(sql_server, snowflake):
    while True:
        sql_ok = sql_server.is_available()
        sf_ok = snowflake.is_available()

        if sql_ok and sf_ok:
            return

        if not sql_ok:
            log("SQL unavailable.")
            sql_server.wait_until_available()

        if not sf_ok:
            log("Snowflake unavailable.")
            snowflake.wait_until_available()


def start_sync_run(sql_server):
    sql = """
        SET NOCOUNT ON;

        INSERT INTO ops.sync_logs (
            start_time,
            end_time,
            status,
            error
        )
        OUTPUT inserted.id AS sync_log_id
        VALUES (
            CAST(
                (SYSUTCDATETIME() AT TIME ZONE 'UTC' AT TIME ZONE 'GMT Standard Time')
                AS DATETIME2
            ),
            NULL,
            'running',
            NULL
        );
    """

    cursor = sql_server.cursor()

    try:
        cursor.execute(sql)
        sync_log_id = cursor.fetchone()[0]
        sql_server.commit()
        return sync_log_id

    except Exception:
        sql_server.rollback()
        raise

    finally:
        cursor.close()


def update_sync_run(sql_server, sync_log_id, status, error=None):
    sql = """
        UPDATE ops.sync_logs
        SET
            end_time = CAST(
                (SYSUTCDATETIME() AT TIME ZONE 'UTC' AT TIME ZONE 'GMT Standard Time')
                AS DATETIME2
            ),
            status = ?,
            error = ?
        WHERE id = ?;
    """

    cursor = sql_server.cursor()

    try:
        cursor.execute(sql, (status, error, sync_log_id))
        sql_server.commit()

    except Exception:
        sql_server.rollback()
        raise

    finally:
        cursor.close()


def start_sync_log_item(sql_server, sync_log_id, schema_name, table_name):
    sql = """
        SET NOCOUNT ON;

        INSERT INTO ops.sync_log_items (
            sync_log_id,
            start_time,
            end_time,
            sync_from,
            status,
            error,
            rows_inserted,
            rows_updated,
            table_name,
            sync_to,
            schema_name,
            last_synced,
            last_pk
        )
        OUTPUT inserted.id
        VALUES (
            ?,
            CAST(
                (SYSUTCDATETIME() AT TIME ZONE 'UTC' AT TIME ZONE 'GMT Standard Time')
                AS DATETIME2
            ),
            NULL,
            NULL,
            'running',
            NULL,
            0,
            0,
            ?,
            NULL,
            ?,
            NULL,
            NULL
        );
    """

    cursor = sql_server.cursor()

    try:
        cursor.execute(
            sql,
            (
                sync_log_id,
                table_name,
                schema_name,
            )
        )

        sync_log_item_id = cursor.fetchone()[0]
        sql_server.commit()

        return sync_log_item_id

    except Exception:
        sql_server.rollback()
        raise

    finally:
        cursor.close()


def update_sync_log_item(
    sql_server,
    sync_log_item_id,
    status,
    rows_inserted=0,
    rows_updated=0,
    last_synced=None,
    last_pk=None,
    error=None
):
    sql = """
        UPDATE ops.sync_log_items
        SET
            end_time = CAST(
                (SYSUTCDATETIME() AT TIME ZONE 'UTC' AT TIME ZONE 'GMT Standard Time')
                AS DATETIME2
            ),
            status = ?,
            rows_inserted = ?,
            rows_updated = ?,
            last_synced = ?,
            last_pk = ?,
            error = ?
        WHERE id = ?;
    """

    cursor = sql_server.cursor()

    try:
        cursor.execute(
            sql,
            (
                status,
                rows_inserted,
                rows_updated,
                last_synced,
                last_pk,
                error,
                sync_log_item_id,
            )
        )

        sql_server.commit()

    except Exception:
        sql_server.rollback()
        raise

    finally:
        cursor.close()


def fetch_snowflake_batch(snowflake, table_name, pk_column, last_synced, last_pk, limit=50_000):
    last_synced_formatted = (
        last_synced.strftime("%Y-%m-%d %H:%M:%S.%f")
        if isinstance(last_synced, (datetime, date))
        else str(last_synced)
    ) + " +0000"

    sql = f"""
        SELECT *
        FROM {table_name}
        WHERE (
            _FIVETRAN_SYNCED > %s
            OR (
                _FIVETRAN_SYNCED = %s
                AND {pk_column} > %s
            )
        )
        ORDER BY _FIVETRAN_SYNCED, {pk_column}
        LIMIT {limit};
    """

    cur = snowflake.cursor()

    try:
        cur.execute(sql, (last_synced_formatted, last_synced_formatted, last_pk))

        columns = [col[0].lower() for col in cur.description]
        rows = cur.fetchall()

        return columns, rows

    finally:
        cur.close()


def ingest_staging_datacore(sql_server, schema_name, table_name, columns, rows):
    if not rows:
        return 0

    target = f"[stg].[{schema_name}_{table_name}]"

    col_sql = ", ".join(f"[{c}]" for c in columns)
    placeholders = ", ".join("?" for _ in columns)

    sql = f"""
        INSERT INTO {target} ({col_sql})
        VALUES ({placeholders});
    """

    cur = sql_server.cursor()
    cur.fast_executemany = True

    try:
        cur.executemany(sql, rows)
        sql_server.commit()
        return len(rows)

    except Exception:
        sql_server.rollback()
        raise

    finally:
        cur.close()


def get_batch_watermark(columns, rows, pk_column):
    if not rows:
        return None, None

    col_lookup = {
        col.lower(): idx
        for idx, col in enumerate(columns)
    }

    synced_idx = col_lookup["_fivetran_synced"]
    pk_idx = col_lookup[pk_column.lower()]

    last_row = rows[-1]

    return last_row[synced_idx], last_row[pk_idx]


def clear_staging_table(sql_server, schema_name, table_name):
    staging_table = f"[stg].[{schema_name}_{table_name}]"

    sql = f"""
        TRUNCATE TABLE {staging_table};
    """

    cur = sql_server.cursor()

    try:
        cur.execute(sql)
        sql_server.commit()

    except Exception:
        sql_server.rollback()
        raise

    finally:
        cur.close()


def merge_staging_to_target(sql_server, schema_name, table_name, pk_column, columns):
    target_table = f"[{schema_name}].[{table_name}]"
    staging_table = f"[stg].[{schema_name}_{table_name}]"

    non_pk_columns = [
        c for c in columns
        if c.lower() != pk_column.lower()
    ]

    if not non_pk_columns:
        raise ValueError(f"No non-PK columns available to update for {schema_name}.{table_name}")

    update_set_sql = ",\n                ".join(
        f"t.[{c}] = s.[{c}]"
        for c in non_pk_columns
    )

    insert_columns_sql = ", ".join(
        f"[{c}]"
        for c in columns
    )

    insert_values_sql = ", ".join(
        f"s.[{c}]"
        for c in columns
    )

    sql = f"""
        SET NOCOUNT ON;

        DECLARE @rows_updated INT = 0;
        DECLARE @rows_inserted INT = 0;

        UPDATE t
        SET
            {update_set_sql}
        FROM {target_table} AS t
        INNER JOIN {staging_table} AS s
            ON t.[{pk_column}] = s.[{pk_column}];

        SET @rows_updated = @@ROWCOUNT;

        INSERT INTO {target_table} (
            {insert_columns_sql}
        )
        SELECT
            {insert_values_sql}
        FROM {staging_table} AS s
        WHERE NOT EXISTS (
            SELECT 1
            FROM {target_table} AS t
            WHERE t.[{pk_column}] = s.[{pk_column}]
        );

        SET @rows_inserted = @@ROWCOUNT;

        SELECT
            @rows_inserted AS rows_inserted,
            @rows_updated AS rows_updated;
    """

    cur = sql_server.cursor()

    try:
        cur.execute(sql)

        result = cur.fetchone()

        rows_inserted = result[0] or 0
        rows_updated = result[1] or 0

        sql_server.commit()

        return {
            "inserted": rows_inserted,
            "updated": rows_updated,
            "total": rows_inserted + rows_updated,
        }

    except Exception:
        sql_server.rollback()
        raise

    finally:
        cur.close()


def fetch_table_config(sql_server, schema_name):
    sql = """
        SELECT
            schema_name,
            table_name,
            primary_key,
            COALESCE(
                last_synced,
                CAST('1900-01-01' AS DATETIME2)
            ) AS last_synced,
            last_pk
        FROM ops.table_config
        WHERE schema_name = ?
        ORDER BY table_name;
    """

    cur = sql_server.cursor()

    try:
        cur.execute(sql, (schema_name,))
        cols = [c[0] for c in cur.description]
        rows = cur.fetchall()

        return [
            dict(zip(cols, row))
            for row in rows
        ]

    finally:
        cur.close()


def update_table_config_watermark(sql_server, schema_name, table_name, last_synced, last_pk):
    sql = """
        UPDATE ops.table_config
        SET
            last_synced = ?,
            last_pk = ?
        WHERE
            schema_name = ?
            AND table_name = ?;
    """

    cur = sql_server.cursor()

    try:
        cur.execute(
            sql,
            (
                last_synced,
                last_pk,
                schema_name,
                table_name,
            )
        )

        sql_server.commit()

    except Exception:
        sql_server.rollback()
        raise

    finally:
        cur.close()

def start_reapit_events_upsert_log(
    sql_server,
    sync_log_item_id,
    rps_instance,
    source_schema_name,
    source_table_name,
    staging_table_name,
    event_table_name,
):
    sql = """
        SET NOCOUNT ON;

        INSERT INTO ops.reapit_events_upsert_logs (
            sync_log_item_id,
            rps_instance,
            source_schema_name,
            source_table_name,
            staging_table_name,
            event_table_name,
            status
        )
        OUTPUT inserted.id
        VALUES (
            ?,
            ?,
            ?,
            ?,
            ?,
            ?,
            'running'
        );
    """

    cur = sql_server.cursor()

    try:
        cur.execute(
            sql,
            (
                sync_log_item_id,
                rps_instance,
                source_schema_name,
                source_table_name,
                staging_table_name,
                event_table_name,
            ),
        )

        log_id = cur.fetchone()[0]
        sql_server.commit()

        return log_id

    except Exception:
        sql_server.rollback()
        raise

    finally:
        cur.close()


def update_reapit_events_upsert_log(
    sql_server,
    log_id,
    status,
    staging_rows=None,
    candidate_event_rows=None,
    rows_inserted=None,
    rows_updated=None,
    rows_deleted=None,
    error=None,
):
    sql = """
        UPDATE ops.reapit_events_upsert_logs
        SET
            status = ?,
            staging_rows = ?,
            candidate_event_rows = ?,
            rows_inserted = ?,
            rows_updated = ?,
            rows_deleted = ?,
            ended_at_utc = sysutcdatetime(),
            error = ?
        WHERE id = ?;
    """

    cur = sql_server.cursor()

    try:
        cur.execute(
            sql,
            (
                status,
                staging_rows,
                candidate_event_rows,
                rows_inserted,
                rows_updated,
                rows_deleted,
                error,
                log_id,
            ),
        )

        sql_server.commit()

    except Exception:
        sql_server.rollback()
        raise

    finally:
        cur.close()


def upsert_certificate_events_from_staging(
    sql_server,
    rps_instance,
    sync_log_item_id,
    source_schema_name,
    source_table_name,
):
    if rps_instance not in {"core", "sa"}:
        raise ValueError(
            f"Invalid rps_instance for certificate event staging upsert: {rps_instance}"
        )

    staging_table_name = f"{source_schema_name}_{source_table_name}"
    event_table_name = "certificate_events"

    log_id = start_reapit_events_upsert_log(
        sql_server=sql_server,
        sync_log_item_id=sync_log_item_id,
        rps_instance=rps_instance,
        source_schema_name=source_schema_name,
        source_table_name=source_table_name,
        staging_table_name=staging_table_name,
        event_table_name=event_table_name,
    )

    sql = """
        EXEC reapit_events.upsert_certificate_events_from_staging
            @rps_instance = ?;
    """

    cur = sql_server.cursor()

    try:
        cur.execute(sql, (rps_instance,))
        row = cur.fetchone()

        result = {
            "staging_rows": row[0] or 0,
            "candidate_event_rows": row[1] or 0,
            "rows_inserted": row[2] or 0,
            "rows_updated": row[3] or 0,
            "rows_deleted": row[4] or 0,
        }

        sql_server.commit()

        update_reapit_events_upsert_log(
            sql_server=sql_server,
            log_id=log_id,
            status="success",
            staging_rows=result["staging_rows"],
            candidate_event_rows=result["candidate_event_rows"],
            rows_inserted=result["rows_inserted"],
            rows_updated=result["rows_updated"],
            rows_deleted=result["rows_deleted"],
            error=None,
        )

        return result

    except Exception as e:
        sql_server.rollback()

        update_reapit_events_upsert_log(
            sql_server=sql_server,
            log_id=log_id,
            status="failed",
            error=truncate_error(e, max_length=1000),
        )

        raise

    finally:
        cur.close()
