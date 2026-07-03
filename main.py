"""
Entry point for the Snowflake -> Datacore replication process.

The script runs the same sync engine against multiple Snowflake instances
(currently Core and SA) and writes into separate SQL Server schemas. Each run:

1. checks both database connections;
2. creates any missing target/staging tables;
3. processes each configured table in batches;
4. records run/table status in ops sync log tables;
5. continues past table-level failures so one bad table does not block the rest.
"""

import time
from datetime import datetime
import traceback
import utils

from clients import SqlServerClient, SnowflakeClient
from configs import datacore_config, core_snowflake_config, sa_snowflake_config

# Number of Snowflake rows to fetch per batch.
# Keep this high enough for throughput, but low enough that staging inserts and
# SQL Server merges remain manageable inside one transaction.
BATCH_LIMIT = 50000

# Intended pause between whole sync cycles if this script is later wrapped in a
# continuous loop/service. Currently the __main__ block performs one pass.
SLEEP_AFTER_SYNC_RUN_SECONDS = 900

# -----------------------------------
# ------------- SYNC ----------------
# -----------------------------------

def sync_table_until_empty(datacore, snowflake, sync_log_id, table_config_row):
    """
    Drain one configured Snowflake table into Datacore until no rows remain.

    The watermark comes from ops.table_config and is advanced only after a
    successful staging insert + target merge. This keeps the process resumable:
    if a batch fails midway, the table will retry from the previous committed
    watermark on the next run.
    """
    total_rows_processed = 0
    batch_number = 0

    schema_name = table_config_row["schema_name"]
    table_name = table_config_row["table_name"]
    pk_column = table_config_row["primary_key"]
    last_synced = table_config_row["last_synced"]
    last_pk = table_config_row["last_pk"]

    utils.log(f"{schema_name}.{table_name}: draining table until empty...")

    while True:
        # Each loop processes one ordered Snowflake batch. The final loop will
        # usually return zero rows, which is the signal that the table is drained.
        batch_number += 1

        utils.log(
            f"{schema_name}.{table_name}: starting batch {batch_number:,} "
            f"from last_synced={last_synced}, last_pk={last_pk}"
        )

        # A separate log item is written per table batch. This makes failures
        # much easier to diagnose than having one giant table-level log row.
        sync_log_item_id = datacore.run_when_available(
            utils.start_sync_log_item,
            sync_log_id=sync_log_id,
            schema_name=schema_name,
            table_name=table_name,
        )

        utils.log(
            f"{schema_name}.{table_name}: sync log item started. "
            f"sync_log_item_id={sync_log_item_id}"
        )

        try:
            utils.log(f"{schema_name}.{table_name}: fetching batch from Snowflake...")

            sf_columns, sf_rows = snowflake.run_when_available(
                utils.fetch_snowflake_batch,
                table_name=table_name,
                pk_column=pk_column,
                last_synced=last_synced,
                last_pk=last_pk,
                limit=BATCH_LIMIT
            )

            # No rows means the current table has caught up to Snowflake. The
            # final empty batch is still logged as success so the audit trail
            # shows that the table was checked, not skipped.
            if not sf_rows:
                datacore.run_when_available(
                    utils.update_sync_log_item,
                    sync_log_item_id=sync_log_item_id,
                    status="success",
                    rows_inserted=0,
                    rows_updated=0,
                    last_synced=last_synced,
                    last_pk=last_pk
                )

                utils.log(
                    f"{schema_name}.{table_name}: no rows returned from Snowflake. "
                    f"Table drained after {batch_number - 1:,} populated batches. "
                    f"Rows processed: {total_rows_processed:,}."
                )

                break

            utils.log(f"{schema_name}.{table_name}: fetched {len(sf_rows):,} rows from Snowflake.")

            # Do not let known bad source rows become staging/merge failures.
            # The batch is fetched first, then split locally: rejected rows are
            # logged to ops.insert_errors, while only keyed rows continue into
            # staging. That way the error log reflects rows we actually saw,
            # not rows hidden by the Snowflake query.
            valid_rows, null_pk_rows = utils.split_rows_by_required_pk(
                columns=sf_columns,
                rows=sf_rows,
                pk_column=pk_column,
            )

            if null_pk_rows:
                rejected_result = datacore.run_when_available(
                    utils.insert_insert_errors,
                    error_reason="NULL_PK",
                    schema_name=schema_name,
                    table_name=table_name,
                    columns=sf_columns,
                    rows=null_pk_rows,
                    sync_log_item_id=sync_log_item_id,
                )

                utils.log(
                    f"{schema_name}.{table_name}: processed "
                    f"{rejected_result['attempted']:,} NULL_PK rejected row(s). "
                    f"Inserted into ops.insert_errors={rejected_result['inserted']:,}, "
                    f"skipped duplicates={rejected_result['skipped_as_duplicates']:,}."
                )

            if not valid_rows:
                datacore.run_when_available(
                    utils.update_sync_log_item,
                    sync_log_item_id=sync_log_item_id,
                    status="success",
                    rows_inserted=0,
                    rows_updated=0,
                    last_synced=last_synced,
                    last_pk=last_pk,
                )

                utils.log(
                    f"{schema_name}.{table_name}: batch contained no valid rows. "
                    f"Rejected rows={len(null_pk_rows):,}. "
                    f"Stopping this table to avoid retrying the same rejected-only batch."
                )

                break

            sf_rows = valid_rows

            total_rows_processed += len(sf_rows)

            utils.log(f"{schema_name}.{table_name}: checking payload columns exist in target/staging...")

            # Fivetran can introduce new columns without warning. Before the
            # insert, compare the current payload against target and staging so
            # additive schema changes are handled automatically.
            snowflake_schema = snowflake.run_when_available(
                type(snowflake).get_table_schema,
                table_name=table_name,
            )

            target_columns_added = datacore.run_when_available(
                utils.ensure_columns_exist_from_payload,
                schema_name=schema_name,
                table_name=table_name,
                sf_columns=sf_columns,
                snowflake_schema=snowflake_schema,
            )

            staging_columns_added = datacore.run_when_available(
                utils.ensure_columns_exist_from_payload,
                schema_name="stg",
                table_name=f"{schema_name}_{table_name}",
                sf_columns=sf_columns,
                snowflake_schema=snowflake_schema,
            )

            if target_columns_added or staging_columns_added:
                utils.log(
                    f"{schema_name}.{table_name}: schema columns added. "
                    f"Target={target_columns_added}, "
                    f"Staging={staging_columns_added}"
                )

            # Staging is deliberately transient. Each batch is loaded into an
            # empty staging table, merged into the target, then replaced by the
            # next batch.
            utils.log(f"{schema_name}.{table_name}: clearing staging table...")

            datacore.run_when_available(
                utils.clear_staging_table,
                schema_name=schema_name,
                table_name=table_name,
            )

            utils.log(f"{schema_name}.{table_name}: inserting {len(sf_rows):,} rows into staging...")

            datacore.run_when_available(
                utils.ingest_staging_datacore,
                schema_name=schema_name,
                table_name=table_name,
                rows=sf_rows,
                columns=sf_columns
            )

            utils.log(f"{schema_name}.{table_name}: merging staging into target...")

            results = datacore.run_when_available(
                utils.merge_staging_to_target,
                schema_name=schema_name,
                table_name=table_name,
                columns=sf_columns,
                pk_column=pk_column
            )

            # JNL is the source for certificate-related events. This derived
            # event table is updated from the same staged batch so it stays in
            # step with the base replication process.
            if schema_name == "reapit" and table_name == "jnl":
                utils.log(f"{schema_name}.{table_name}: upserting certificate events from staging...")

                event_result = datacore.run_when_available(
                    utils.upsert_certificate_events_from_staging,
                    rps_instance="core",
                    sync_log_item_id=sync_log_item_id,
                    source_schema_name=schema_name,
                    source_table_name=table_name,
                )

                utils.log(
                    f"{schema_name}.{table_name}: certificate events upsert completed. "
                    f"Staging rows={event_result['staging_rows']:,}, "
                    f"candidate events={event_result['candidate_event_rows']:,}, "
                    f"inserted={event_result['rows_inserted']:,}, "
                    f"updated={event_result['rows_updated']:,}, "
                    f"deleted={event_result['rows_deleted']:,}."
                )

            elif schema_name == "reapit_sa" and table_name == "jnl":
                utils.log(f"{schema_name}.{table_name}: upserting certificate events from staging...")

                event_result = datacore.run_when_available(
                    utils.upsert_certificate_events_from_staging,
                    rps_instance="sa",
                    sync_log_item_id=sync_log_item_id,
                    source_schema_name=schema_name,
                    source_table_name=table_name,
                )

                utils.log(
                    f"{schema_name}.{table_name}: certificate events upsert completed. "
                    f"Staging rows={event_result['staging_rows']:,}, "
                    f"candidate events={event_result['candidate_event_rows']:,}, "
                    f"inserted={event_result['rows_inserted']:,}, "
                    f"updated={event_result['rows_updated']:,}, "
                    f"deleted={event_result['rows_deleted']:,}."
                )

            # Watermark must be based on the final row in the ordered batch.
            # It is stored only after the merge succeeds.
            last_synced, last_pk = utils.get_batch_watermark(
                columns=sf_columns,
                rows=sf_rows,
                pk_column=pk_column
            )

            utils.log(
                f"{schema_name}.{table_name}: batch {batch_number:,} merged. "
                f"Inserted={results['inserted']:,}, "
                f"updated={results['updated']:,}, "
                f"new_last_synced={last_synced}, "
                f"new_last_pk={last_pk}"
            )

            datacore.run_when_available(
                utils.update_sync_log_item,
                sync_log_item_id=sync_log_item_id,
                status="success",
                rows_inserted=results["inserted"],
                rows_updated=results["updated"],
                last_synced=last_synced,
                last_pk=last_pk
            )

            utils.log(f"{schema_name}.{table_name}: sync log item updated successfully.")

            datacore.run_when_available(
                utils.update_table_config_watermark,
                schema_name=schema_name,
                table_name=table_name,
                last_synced=last_synced,
                last_pk=last_pk
            )

            utils.log(f"{schema_name}.{table_name}: table_config watermark updated.")

        except Exception as e:
            # Mark this batch as failed, then re-raise so the caller can decide
            # whether to continue with the next table or fail the whole run.
            utils.log(f"batch {batch_number} failed: {type(e).__name__}: {repr(e)}")
            utils.log(traceback.format_exc())

            datacore.run_when_available(
                utils.update_sync_log_item,
                sync_log_item_id=sync_log_item_id,
                status="failed",
                rows_inserted=0,
                rows_updated=0,
                error=utils.truncate_error(e, max_length=1000)
            )

            raise

    return total_rows_processed


# -----------------------------------
# ---------- SYNC ERRORS ------------
# -----------------------------------

def build_failed_tables_error(failed_tables: list[str], max_length: int = 1000) -> str | None:
    """
    Build a compact run-level error summary from table-level failures.

    ops.sync_logs.error has finite space, so long lists are truncated while
    preserving the failed-table count at the front.
    """
    if not failed_tables:
        return None

    unique_failed_tables = sorted(set(failed_tables))

    prefix = f"{len(unique_failed_tables)} table(s) failed: "
    table_text = ", ".join(unique_failed_tables)
    error_text = prefix + table_text

    if len(error_text) <= max_length:
        return error_text

    suffix = "... [truncated]"
    keep_length = max_length - len(prefix) - len(suffix)

    if keep_length <= 0:
        return error_text[:max_length]

    return prefix + table_text[:keep_length] + suffix

# -----------------------------------
# ------------- MAIN ----------------
# -----------------------------------

def main(datacore, snowflake, sync_schema_name):
    """
    Run one full sync cycle for a single Snowflake instance/schema pair.

    Table-level failures are collected and reported as completed_with_errors;
    only failures before/during orchestration are treated as run-level failure.
    """
    utils.log(f"Checking SQL Server and Snowflake connections for {sync_schema_name}...")
    utils.wait_for_all_connections(datacore, snowflake)

    utils.log(f"Handling database schema changes for {sync_schema_name}...")
    schema_change_result = utils.handle_schema_changes(
        snowflake=snowflake,
        datacore=datacore,
        sync_schema_name=sync_schema_name,
        staging_schema_name="stg",
    )

    utils.log(
        f"Schema handling complete for {sync_schema_name}. "
        f"Target tables added: {len(schema_change_result['target_tables_added'])}. "
        f"Staging tables added: {len(schema_change_result['staging_tables_added'])}."
    )

    utils.log(f"Starting sync run for {sync_schema_name}...")
    sync_log_id = datacore.run_when_available(utils.start_sync_run)
    utils.log(f"Sync run started for {sync_schema_name}. sync_log_id={sync_log_id}")

    total_rows_processed = 0
    failed_tables = []

    try:
        table_config = datacore.run_when_available(
            utils.fetch_table_config,
            schema_name=sync_schema_name,
        )

        utils.log(
            f"Fetched {len(table_config):,} tables from ops.table_config "
            f"for schema '{sync_schema_name}'."
        )

        # Process tables serially. This is intentionally boring: fewer moving
        # parts, simpler SQL locking behaviour, and clearer failure recovery.
        for index, row in enumerate(table_config, start=1):
            schema_name = row["schema_name"]
            table_name = row["table_name"]
            full_table_name = f"{schema_name}.{table_name}"

            utils.log(
                f"Starting table {index:,}/{len(table_config):,}: "
                f"{full_table_name} "
                f"pk={row['primary_key']} "
                f"last_synced={row['last_synced']} "
                f"last_pk={row['last_pk']}"
            )

            try:
                rows_processed = sync_table_until_empty(
                    datacore=datacore,
                    snowflake=snowflake,
                    sync_log_id=sync_log_id,
                    table_config_row=row,
                )

                total_rows_processed += rows_processed

                utils.log(
                    f"Finished table {full_table_name}. "
                    f"Rows processed this table: {rows_processed:,}. "
                    f"Run total: {total_rows_processed:,}."
                )

            except Exception as table_error:
                # A single bad table should not prevent unrelated tables from
                # syncing. The run is marked completed_with_errors at the end.
                failed_tables.append(full_table_name)

                utils.log(
                    f"Table failed: {full_table_name}. "
                    f"Error: {table_error}. "
                    f"Continuing with next table..."
                )

                continue

        if failed_tables:
            error_summary = build_failed_tables_error(
                failed_tables,
                max_length=1000,
            )

            datacore.run_when_available(
                utils.update_sync_run,
                sync_log_id=sync_log_id,
                status="completed_with_errors",
                error=error_summary,
            )

            utils.log(
                f"Sync run marked as completed_with_errors for {sync_schema_name}. "
                f"sync_log_id={sync_log_id}. "
                f"Failed tables: {len(set(failed_tables)):,}. "
                f"Rows processed: {total_rows_processed:,}. "
                f"{error_summary}"
            )

        else:
            datacore.run_when_available(
                utils.update_sync_run,
                sync_log_id=sync_log_id,
                status="completed",
                error=None,
            )

            utils.log(
                f"Sync run marked as completed for {sync_schema_name}. "
                f"sync_log_id={sync_log_id}. "
                f"Rows processed: {total_rows_processed:,}."
            )

        return total_rows_processed

    except Exception as e:
        utils.log(
            f"Sync run failed catastrophically for {sync_schema_name}. "
            f"sync_log_id={sync_log_id}. Error: {e}"
        )

        datacore.run_when_available(
            utils.update_sync_run,
            sync_log_id=sync_log_id,
            status="failed",
            error=f"Run-level failure before table sync completed: {type(e).__name__}",
        )

        raise


if __name__ == "__main__":
    # One shared SQL Server connection is reused for both Snowflake instances.
    datacore = SqlServerClient(**datacore_config)

    # Each source instance maps to its own target schema. The same sync logic is
    # reused; only connection details and schema names differ.
    snowflake_instances = [
        {
            "name": "core",
            "client": SnowflakeClient(**core_snowflake_config),
            "sync_schema_name": "reapit",
        },
        {
            "name": "sa",
            "client": SnowflakeClient(**sa_snowflake_config),
            "sync_schema_name": "reapit_sa",
        },
    ]

    for sf in snowflake_instances:
        try:
            utils.log(
                f"Starting Snowflake sync instance: "
                f"{sf['name']} -> {sf['sync_schema_name']}"
            )

            rows_processed = main(
                datacore=datacore,
                snowflake=sf["client"],
                sync_schema_name=sf["sync_schema_name"],
            )

            utils.log(
                f"Sync instance completed: "
                f"{sf['name']} -> {sf['sync_schema_name']}. "
                f"Rows processed: {rows_processed:,}"
            )

        except Exception as e:
            utils.log(
                f"Sync instance failed: "
                f"{sf['name']} -> {sf['sync_schema_name']}. "
                f"Error: {e}"
            )

    utils.log("All Snowflake instances processed.")
