/*
    Snowflake -> Datacore sync SQL prerequisites.

    00_schemas.sql

    Run this first in the target Datacore database.

    Notes:
    - Hard-coded database names are deliberately avoided so the same scripts can
      run in test/prod without editing object references.
    - The replicated reapit/reapit_sa tables are not scripted here. The Python
      sync creates and evolves those from Snowflake metadata.
*/

IF SCHEMA_ID('ops') IS NULL
    EXEC('CREATE SCHEMA ops');
GO

IF SCHEMA_ID('reapit') IS NULL
    EXEC('CREATE SCHEMA reapit');
GO

IF SCHEMA_ID('reapit_sa') IS NULL
    EXEC('CREATE SCHEMA reapit_sa');
GO

IF SCHEMA_ID('stg') IS NULL
    EXEC('CREATE SCHEMA stg');
GO

IF SCHEMA_ID('reapit_events') IS NULL
    EXEC('CREATE SCHEMA reapit_events');
GO
