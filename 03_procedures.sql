/*
    Snowflake -> Datacore sync SQL prerequisites.

    03_procedures.sql

    Run after 01_tables.sql.

    This procedure maintains the derived reapit_events.certificate_events table
    from the current staged JNL batch. The Python sync calls it after each JNL
    staging-to-target merge for both Core and SA.
*/

CREATE OR ALTER PROCEDURE reapit_events.upsert_certificate_events_from_staging
    @rps_instance varchar(40)
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE @staging_rows int = 0;
    DECLARE @candidate_event_rows int = 0;
    DECLARE @rows_inserted int = 0;
    DECLARE @rows_updated int = 0;
    DECLARE @rows_deleted int = 0;

    IF @rps_instance NOT IN ('core', 'sa')
    BEGIN
        THROW 51001, 'Invalid @rps_instance. Expected core or sa.', 1;
    END;

    CREATE TABLE #staged_jnl (
        [_fivetran_id] varchar(256) NOT NULL,
        jnl_register datetime2 NULL,
        jnl_entrytype varchar(20) NULL,
        jnl_entry nvarchar(MAX) NULL,
        jnl_synchdel bit NULL,
        prpcode varchar(9) NULL,
        negcode varchar(4) NULL
    );

    CREATE TABLE #source_events (
        [_fivetran_id] varchar(256) NOT NULL,
        jnl_register datetime2 NULL,
        jnl_entrytype varchar(20) NULL,
        jnl_entry nvarchar(MAX) NULL,
        jnl_synchdel bit NULL,
        prpcode varchar(9) NULL,
        negcode varchar(4) NULL,
        certificate_type varchar(20) NOT NULL,
        event_type varchar(50) NOT NULL
    );

    IF @rps_instance = 'core'
    BEGIN
        INSERT INTO #staged_jnl (
            [_fivetran_id],
            jnl_register,
            jnl_entrytype,
            jnl_entry,
            jnl_synchdel,
            prpcode,
            negcode
        )
        SELECT
            CAST(j.[_fivetran_id] AS varchar(256)),
            j.register,
            j.entrytype,
            j.entry,
            j.synchdel,
            j.prpcode,
            j.negcode
        FROM stg.reapit_jnl AS j;

        SET @staging_rows = @@ROWCOUNT;
    END
    ELSE IF @rps_instance = 'sa'
    BEGIN
        INSERT INTO #staged_jnl (
            [_fivetran_id],
            jnl_register,
            jnl_entrytype,
            jnl_entry,
            jnl_synchdel,
            prpcode,
            negcode
        )
        SELECT
            CAST(j.[_fivetran_id] AS varchar(256)),
            j.register,
            j.entrytype,
            j.entry,
            j.synchdel,
            j.prpcode,
            j.negcode
        FROM stg.reapit_sa_jnl AS j;

        SET @staging_rows = @@ROWCOUNT;
    END;

    INSERT INTO #source_events (
        [_fivetran_id],
        jnl_register,
        jnl_entrytype,
        jnl_entry,
        jnl_synchdel,
        prpcode,
        negcode,
        certificate_type,
        event_type
    )
    SELECT
        s.[_fivetran_id],
        s.jnl_register,
        s.jnl_entrytype,
        s.jnl_entry,
        s.jnl_synchdel,
        s.prpcode,
        s.negcode,
        'GS' AS certificate_type,
        'added' AS event_type
    FROM #staged_jnl AS s
    WHERE s.jnl_entry LIKE 'Gas Safety certificate added%';

    SET @candidate_event_rows = @@ROWCOUNT;

    DELETE target
    FROM reapit_events.certificate_events AS target
    INNER JOIN #staged_jnl AS s
        ON target.rps_instance = @rps_instance
       AND target.[_fivetran_id] = s.[_fivetran_id]
    WHERE target.certificate_type = 'GS'
      AND target.event_type = 'added'
      AND NOT EXISTS (
          SELECT 1
          FROM #source_events AS source
          WHERE source.[_fivetran_id] = s.[_fivetran_id]
      );

    SET @rows_deleted = @@ROWCOUNT;

    UPDATE target
    SET
        target.jnl_register = source.jnl_register,
        target.jnl_entrytype = source.jnl_entrytype,
        target.jnl_entry = source.jnl_entry,
        target.jnl_synchdel = source.jnl_synchdel,
        target.prpcode = source.prpcode,
        target.negcode = source.negcode,
        target.certificate_type = source.certificate_type,
        target.event_type = source.event_type,
        target.updated_at_utc = SYSUTCDATETIME()
    FROM reapit_events.certificate_events AS target
    INNER JOIN #source_events AS source
        ON target.rps_instance = @rps_instance
       AND target.[_fivetran_id] = source.[_fivetran_id];

    SET @rows_updated = @@ROWCOUNT;

    INSERT INTO reapit_events.certificate_events (
        rps_instance,
        [_fivetran_id],
        jnl_register,
        jnl_entrytype,
        jnl_entry,
        jnl_synchdel,
        prpcode,
        negcode,
        certificate_type,
        event_type
    )
    SELECT
        @rps_instance AS rps_instance,
        source.[_fivetran_id],
        source.jnl_register,
        source.jnl_entrytype,
        source.jnl_entry,
        source.jnl_synchdel,
        source.prpcode,
        source.negcode,
        source.certificate_type,
        source.event_type
    FROM #source_events AS source
    WHERE NOT EXISTS (
        SELECT 1
        FROM reapit_events.certificate_events AS target
        WHERE target.rps_instance = @rps_instance
          AND target.[_fivetran_id] = source.[_fivetran_id]
    );

    SET @rows_inserted = @@ROWCOUNT;

    SELECT
        @staging_rows AS staging_rows,
        @candidate_event_rows AS candidate_event_rows,
        @rows_inserted AS rows_inserted,
        @rows_updated AS rows_updated,
        @rows_deleted AS rows_deleted;
END;
GO
