import time
from datetime import datetime
import traceback
import utils

from clients import SqlServerClient, SnowflakeClient
from configs import datacore_config, core_snowflake_config, sa_snowflake_config

BATCH_LIMIT = 50000
SLEEP_AFTER_SYNC_RUN_SECONDS = 900

# -----------------------------------
# ------------- SYNC ----------------
# -----------------------------------

def sync_table_until_empty(datacore, snowflake, sync_log_id, table_config_row):
    total_rows_processed = 0
    batch_number = 0

    schema_name = table_config_row["schema_name"]
    table_name = table_config_row["table_name"]
    pk_column = table_config_row["primary_key"]
    last_synced = table_config_row["last_synced"]
    last_pk = table_config_row["last_pk"]

    utils.log(f"{schema_name}.{table_name}: draining table until empty...")

    while True:
        batch_number += 1

        utils.log(
            f"{schema_name}.{table_name}: starting batch {batch_number:,} "
            f"from last_synced={last_synced}, last_pk={last_pk}"
        )

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

            total_rows_processed += len(sf_rows)

            utils.log(f"{schema_name}.{table_name}: checking payload columns exist in target/staging...")

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

            last_synced, last_pk = utils.get_batch_watermark(
                columns=sf_columns,
                rows=sf_rows,
                pk_column=pk_column
            )

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
    datacore = SqlServerClient(**datacore_config)

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
