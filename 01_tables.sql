/*
    Snowflake -> Datacore sync SQL prerequisites.

    01_tables.sql

    Run after 00_schemas.sql.

    This script creates the operational tables required by the Python sync.
    It does not create replicated reapit/reapit_sa/stg data tables; those are
    created dynamically by the sync from Snowflake metadata.
*/

IF OBJECT_ID('ops.db_schema_evo_log', 'U') IS NULL
BEGIN
    CREATE TABLE ops.db_schema_evo_log (
        id int IDENTITY(1,1) NOT NULL,
        created datetime2 NOT NULL
            CONSTRAINT df_ops_db_schema_evo_log_created
            DEFAULT CONVERT(datetime2, ((SYSUTCDATETIME() AT TIME ZONE 'UTC') AT TIME ZONE 'GMT Standard Time')),
        schema_name nvarchar(255) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
        table_name nvarchar(255) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
        CONSTRAINT pk_ops_db_schema_evo_log PRIMARY KEY (id)
    );
END;
GO

IF OBJECT_ID('ops.insert_errors', 'U') IS NULL
BEGIN
    CREATE TABLE ops.insert_errors (
        id bigint IDENTITY(1,1) NOT NULL,
        error_reason nvarchar(200) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
        payload nvarchar(MAX) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
        created_at datetimeoffset NOT NULL
            CONSTRAINT df_ops_insert_errors_created_at
            DEFAULT (SYSUTCDATETIME() AT TIME ZONE 'GMT Standard Time'),
        sync_log_item_id int NULL,
        error_hash varbinary(32) NULL,
        CONSTRAINT pk_ops_insert_errors PRIMARY KEY (id)
    );
END;
GO

IF OBJECT_ID('ops.insert_errors', 'U') IS NOT NULL
AND NOT EXISTS (
    SELECT 1
    FROM sys.indexes
    WHERE name = 'ux_insert_errors_hash'
      AND object_id = OBJECT_ID('ops.insert_errors')
)
BEGIN
    CREATE UNIQUE NONCLUSTERED INDEX ux_insert_errors_hash
    ON ops.insert_errors (error_hash ASC)
    WHERE error_hash IS NOT NULL;
END;
GO

IF OBJECT_ID('ops.reapit_events_upsert_logs', 'U') IS NULL
BEGIN
    CREATE TABLE ops.reapit_events_upsert_logs (
        id bigint IDENTITY(1,1) NOT NULL,
        sync_log_item_id bigint NULL,
        rps_instance varchar(40) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
        source_schema_name varchar(128) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
        source_table_name varchar(128) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
        staging_table_name varchar(256) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
        event_table_name varchar(128) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
        status varchar(30) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
        staging_rows int NULL,
        candidate_event_rows int NULL,
        rows_inserted int NULL,
        rows_updated int NULL,
        rows_deleted int NULL,
        started_at_utc datetime2 NOT NULL
            CONSTRAINT df_ops_reapit_events_upsert_logs_started_at_utc
            DEFAULT SYSUTCDATETIME(),
        ended_at_utc datetime2 NULL,
        error nvarchar(1000) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
        CONSTRAINT pk_reapit_events_upsert_logs PRIMARY KEY (id)
    );
END;
GO

IF OBJECT_ID('ops.sync_log_items', 'U') IS NULL
BEGIN
    CREATE TABLE ops.sync_log_items (
        id int IDENTITY(1,1) NOT NULL,
        sync_log_id int NOT NULL,
        start_time datetime2 NOT NULL,
        end_time datetime2 NULL,
        sync_from datetime2 NULL,
        status nvarchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
        error nvarchar(1000) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
        rows_inserted int NULL,
        rows_updated int NULL,
        table_name nvarchar(256) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
        sync_to datetime2 NULL,
        schema_name nvarchar(128) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
        last_synced datetime2(0) NULL,
        last_pk nvarchar(128) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
        CONSTRAINT pk_ops_sync_log_items PRIMARY KEY (id)
    );
END;
GO

IF OBJECT_ID('ops.sync_log_items', 'U') IS NOT NULL
AND NOT EXISTS (
    SELECT 1
    FROM sys.indexes
    WHERE name = 'idx_sync_log_items_table_synced'
      AND object_id = OBJECT_ID('ops.sync_log_items')
)
BEGIN
    CREATE NONCLUSTERED INDEX idx_sync_log_items_table_synced
    ON ops.sync_log_items (table_name ASC, sync_to DESC);
END;
GO

IF OBJECT_ID('ops.sync_logs', 'U') IS NULL
BEGIN
    CREATE TABLE ops.sync_logs (
        id int IDENTITY(1,1) NOT NULL,
        start_time datetime2 NOT NULL,
        end_time datetime2 NULL,
        status nvarchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
        error nvarchar(1000) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
        CONSTRAINT pk_ops_sync_logs PRIMARY KEY (id)
    );
END;
GO

IF OBJECT_ID('ops.table_config', 'U') IS NULL
BEGIN
    CREATE TABLE ops.table_config (
        table_name nvarchar(128) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
        primary_key nvarchar(128) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
        schema_name nvarchar(128) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
        last_pk nvarchar(128) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
        last_synced datetime2 NULL
    );
END;
GO

IF OBJECT_ID('ops.tbl_schema_evo_log', 'U') IS NULL
BEGIN
    CREATE TABLE ops.tbl_schema_evo_log (
        id int IDENTITY(1,1) NOT NULL,
        created datetime2 NOT NULL
            CONSTRAINT df_ops_tbl_schema_evo_log_created
            DEFAULT CONVERT(datetime2, ((SYSUTCDATETIME() AT TIME ZONE 'UTC') AT TIME ZONE 'GMT Standard Time')),
        schema_name nvarchar(255) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
        table_name nvarchar(255) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
        column_name nvarchar(255) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
        CONSTRAINT pk_ops_tbl_schema_evo_log PRIMARY KEY (id)
    );
END;
GO

/*
    Reapit certificate-event table.

    This is a derived operational table maintained from staged JNL rows after
    each JNL batch merge. It is not created dynamically by the Python sync,
    so it must exist before the JNL certificate-event upsert can run.
*/

IF OBJECT_ID('reapit_events.certificate_events', 'U') IS NULL
BEGIN
    CREATE TABLE reapit_events.certificate_events (
        rps_instance varchar(40) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
        [_fivetran_id] varchar(256) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
        jnl_register datetime2 NULL,
        jnl_register_date AS (CONVERT(date, [jnl_register])) PERSISTED,
        jnl_entrytype varchar(20) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
        jnl_entry nvarchar(MAX) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
        jnl_synchdel bit NULL,
        prpcode varchar(9) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
        negcode varchar(4) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
        certificate_type varchar(20) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
        event_type varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
        created_at_utc datetime2 NOT NULL
            CONSTRAINT df_reapit_certificate_events_created_at_utc
            DEFAULT SYSUTCDATETIME(),
        updated_at_utc datetime2 NOT NULL
            CONSTRAINT df_reapit_certificate_events_updated_at_utc
            DEFAULT SYSUTCDATETIME(),
        CONSTRAINT pk_reapit_certificate_events
            PRIMARY KEY (rps_instance, [_fivetran_id])
    );
END;
GO

IF OBJECT_ID('reapit_events.certificate_events', 'U') IS NOT NULL
AND NOT EXISTS (
    SELECT 1
    FROM sys.indexes
    WHERE name = 'ix_certificate_events_register_window'
      AND object_id = OBJECT_ID('reapit_events.certificate_events')
)
BEGIN
    CREATE NONCLUSTERED INDEX ix_certificate_events_register_window
    ON reapit_events.certificate_events (
        rps_instance ASC,
        prpcode ASC,
        certificate_type ASC,
        event_type ASC,
        jnl_register ASC
    )
    INCLUDE (negcode);
END;
GO

IF OBJECT_ID('reapit_events.certificate_events', 'U') IS NOT NULL
AND NOT EXISTS (
    SELECT 1
    FROM sys.indexes
    WHERE name = 'ix_reapit_certificate_events_cert_match'
      AND object_id = OBJECT_ID('reapit_events.certificate_events')
)
BEGIN
    CREATE NONCLUSTERED INDEX ix_reapit_certificate_events_cert_match
    ON reapit_events.certificate_events (
        rps_instance ASC,
        prpcode ASC,
        certificate_type ASC,
        event_type ASC,
        jnl_register_date ASC
    )
    INCLUDE (jnl_register, negcode);
END;
GO

IF OBJECT_ID('reapit_events.certificate_events', 'U') IS NOT NULL
AND NOT EXISTS (
    SELECT 1
    FROM sys.indexes
    WHERE name = 'ix_reapit_certificate_events_type'
      AND object_id = OBJECT_ID('reapit_events.certificate_events')
)
BEGIN
    CREATE NONCLUSTERED INDEX ix_reapit_certificate_events_type
    ON reapit_events.certificate_events (
        certificate_type ASC,
        event_type ASC,
        rps_instance ASC
    );
END;
GO

