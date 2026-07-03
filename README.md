# Snowflake -> Datacore sync package

## Contents

- `src/` - Python sync script and supporting modules.
- `sql/00_schemas.sql` - prerequisite schemas: `ops`, `reapit`, `reapit_sa`, `stg`, and `reapit_events`.
- `sql/01_tables.sql` - operational tables, `reapit_events.certificate_events`, and supporting indexes.
- `sql/02_views.sql` - utility storage views.
- `sql/03_procedures.sql` - `reapit_events.upsert_certificate_events_from_staging`.
- `.env.example` - required environment variables.
- `requirements.txt` - Python dependencies.

## SQL deployment order

1. `sql/00_schemas.sql`
2. `sql/01_tables.sql`
3. `sql/03_procedures.sql`
4. `sql/02_views.sql`

The replicated `reapit.*`, `reapit_sa.*`, and `stg.*` data tables are not scripted because the Python sync creates/evolves those from Snowflake metadata.

The `reapit_events` objects are included because they are dependencies of the JNL certificate-event step. The Python sync calls:

```sql
EXEC reapit_events.upsert_certificate_events_from_staging
    @rps_instance = ?;
```
