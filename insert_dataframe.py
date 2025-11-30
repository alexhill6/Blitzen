"""Insert dataframe function implementation"""

import logging
import psycopg2

logger = logging.getLogger("insertion_logger")
logger.setLevel(logging.INFO)

# Write all errors to a separate file
file_handler = logging.FileHandler("insertion_errors.log")
file_handler.setLevel(logging.ERROR)

# Include timestamps and details
formatter = logging.Formatter(
    "%(asctime)s - [%(levelname)s] - %(message)s"
)
file_handler.setFormatter(formatter)

logger.addHandler(file_handler)


def insert_dataframe(df, table_name, host_name, db_name, user_name, pw):
    """
    Insert all rows from df into table_name in fixed batches of 500.

    - Each batch is inserted in its own transaction.
    - If ANY row in a batch fails, the entire batch is rolled back and skipped.
    - Any errors are written to insertion_errors.log.
    """

    batch_size = 500  # FIXED batch size

    columns = list(df.columns)
    col_names = ", ".join(columns)
    placeholders = ", ".join([f"%({c})s" for c in columns])

    if table_name == "institution_ipeds_info":
        conflict_cols = ["UNITID"]
    else:
        conflict_cols = ["UNITID", "YEAR"]

    conflict_clause = ", ".join(conflict_cols)

    set_updates = ", ".join([f"{c} = EXCLUDED.{c}" for c in columns])
    insert_sql = f"""
        INSERT INTO {table_name} ({col_names})
        VALUES ({placeholders})
        ON CONFLICT ({conflict_clause})
        DO UPDATE SET {set_updates};
    """

    rows_to_insert = df.to_dict("records")
    total_rows = len(rows_to_insert)

    print(
        f"\nAttempting to insert {total_rows} rows into {table_name} "
        f"in batches of {batch_size}."
    )

    try:
        with psycopg2.connect(
            host=host_name,
            dbname=db_name,
            user=user_name,
            password=pw
        ) as conn:
            conn.autocommit = False

            for i in range(0, total_rows, batch_size):
                batch_start_index = i
                batch_end_index = min(i + batch_size, total_rows)
                batch = rows_to_insert[batch_start_index:batch_end_index]

                print(
                    f"Processing batch {batch_start_index + 1} to {batch_end_index} "
                    f"(size: {len(batch)})...",
                    end=""
                )

                try:
                    with conn.cursor() as cur:
                        cur.executemany(insert_sql, batch)

                    conn.commit()
                    print("COMMITTED.")

                except Exception as e:
                    conn.rollback()
                    print("ROLLED BACK.")

                    # Log the failed batch
                    logger.error(
                        "Batch FAILED for table '%s': rows %d–%d. Error: %s",
                        table_name,
                        batch_start_index,
                        batch_end_index,
                        str(e),
                    )

                    print("\nERROR inserting batch:")
                    print(f"  Batch index range: {batch_start_index} to {batch_end_index}")
                    print(f"  Postgres error: {e}")
                    print("Attempting single-row re-insertion to find the culprit...")

                    culprit_found = False

                    # Row-by-row fallback
                    for idx_in_batch, row_dict in enumerate(batch):
                        try:
                            with conn.cursor() as cur_check:
                                cur_check.execute(insert_sql, row_dict)
                            conn.commit()
                        except Exception as inner_e:
                            conn.rollback()

                            df_idx = df.index[batch_start_index + idx_in_batch]

                            print("\nCULPRIT ROW IDENTIFIED:")
                            print(f"  Original DataFrame Index: {df_idx}")
                            if "UNITID" in row_dict:
                                print(f"  UNITID: {row_dict['UNITID']}")
                            print(f"  Postgres error: {inner_e}")
                            print(
                                f"  This batch ({batch_start_index + 1} to {batch_end_index}) "
                                f"is fully SKIPPED."
                            )

                            logger.error(
                                "Culprit row in table '%s': df_index=%s, UNITID=%s, Error=%s",
                                table_name,
                                df_idx,
                                row_dict.get("UNITID"),
                                str(inner_e),
                            )

                            culprit_found = True
                            break

                    if not culprit_found:
                        print("Could not isolate the single failing row in this batch.")
                        logger.error(
                            "Could not isolate failing row for table '%s' in batch rows %d–%d.",
                            table_name,
                            batch_start_index,
                            batch_end_index,
                        )

            print(f"\nCompleted insertion into {table_name}. Successfully inserted all valid batches.")

    except Exception as e:
        print("Connection or top-level error during batching process:")
        print(e)
        logger.error(
            "Top-level connection error for table '%s': %s",
            table_name,
            str(e),
        )
