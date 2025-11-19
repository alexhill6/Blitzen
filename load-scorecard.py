import sys
import pandas as pd
import psycopg2
import numpy as np
from sqlalchemy import create_engine
import importlib.util
import credentials_copy

scorecard_df = pd.read_csv(sys.argv[1])

year = int(sys.argv[1][-14:-10]) + 1

scorecard_df['YEAR'] = year

institution_financial_df = scorecard_df[['UNITID', 'YEAR', 'TUITIONFEE_IN',
                                         'TUITIONFEE_OUT', 'TUITIONFEE_PROG',
                                         'TUITFTE', 'AVGFACSAL', 
                                         'CDR2', 'CDR3']]
institution_scorecard_info_df = scorecard_df[['UNITID', 'YEAR', 'ACCREDAGENCY',
                                              'PREDDEG', 'HIGHDEG',
                                              'CONTROL', 'REGION']]
institution_admissions_df = scorecard_df[['UNITID', 'YEAR', 'ADM_RATE',
                                          'SATVR25', 'SATVR75', 'SATMT25',
                                          'SATMT75', 'SATVRMID', 'SATMTMID',
                                          'ACTCM25', 'ACTCM75', 'ACTEN25',
                                          'ACTEN75', 'ACTMT25', 'ACTMT75',
                                          'ACTCMMID', 'ACTENMID', 
                                          'ACTMTMID', 'SAT_AVG']]
institution_completion_df = scorecard_df[['UNITID', 'YEAR', 'C150_4',
                                          'C150_4_WHITE', 'C150_4_BLACK',
                                          'C150_4_HISP', 'C150_4_ASIAN',
                                          'C150_4_AIAN', 'C150_4_NHPI',
                                          'C150_4_2MOR', 'C150_4_NRA',
                                          'C150_4_UNKN']]

institution_financial_df = institution_financial_df.where(
    pd.notna(institution_financial_df), None
    )
institution_scorecard_info_df = institution_scorecard_info_df.where(
    pd.notna(institution_scorecard_info_df), None
    )
institution_admissions_df = institution_admissions_df.where(
    pd.notna(institution_admissions_df), None
    )
institution_completion_df = institution_completion_df.where(
    pd.notna(institution_completion_df), None
    )
institution_financial_df = institution_financial_df.replace(
    {pd.NA: None, np.nan: None}
    )
institution_scorecard_info_df = institution_scorecard_info_df.replace(
    {pd.NA: None, np.nan: None}
    )
institution_admissions_df = institution_admissions_df.replace(
    {pd.NA: None, np.nan: None}
    )
institution_completion_df = institution_completion_df.replace(
    {pd.NA: None, np.nan: None}
    )


username = credentials_copy.DB_USER
database = credentials_copy.DB_USER
password = credentials_copy.DB_PASSWORD
db_url = f"postgresql://" + username + ":" + password +
"@debprodserver.postgres.database.azure.com:5432/" + database
engine = create_engine(db_url)

if "UNITID" not in scorecard_df.columns:
    raise ValueError("DataFrame is missing 'unitid' column")

# 2. Fetch valid unitids from IPEDS parent table
host = "debprodserver.postgres.database.azure.com"
dbname = credentials_copy.DB_USER

with psycopg2.connect(
    host=host,
    dbname=dbname,
    user=username,
    password=password
) as conn:
    valid_unitids = pd.read_sql(
        "SELECT unitid FROM institution_ipeds_info;",
        conn
    )["unitid"]

valid_unitid_set = set(valid_unitids.tolist())

# 3. Filter to only rows whose unitid exists in IPEDS
institution_financial_df = institution_financial_df[institution_financial_df["UNITID"].isin(valid_unitid_set)].copy()
institution_scorecard_info_df = institution_scorecard_info_df[institution_scorecard_info_df["UNITID"].isin(valid_unitid_set)].copy()
institution_admissions_df = institution_admissions_df[institution_admissions_df["UNITID"].isin(valid_unitid_set)].copy()
institution_completion_df = institution_completion_df[institution_completion_df["UNITID"].isin(valid_unitid_set)].copy()

kept = len(institution_financial_df)
total_read = len(institution_financial_df)
dropped = total_read - kept

print(f"{kept} rows have matching unitid in institution_ipeds_info.")
print(f"{dropped} rows dropped (no matching unitid in IPEDS).")

def insert_dataframe_strict(df, table_name, host_name, db_name, user_name, pw):
    """
    Insert all rows from df into table_name in ONE transaction.

    - If ANY row fails, roll back the entire transaction and stop.
    - Prints which row failed (index + identifying info).
    """

    columns = list(df.columns)
    col_names = ", ".join(columns)
    placeholders = ", ".join([f"%({c})s" for c in columns])

    insert_sql = f"""
        INSERT INTO {table_name} ({col_names})
        VALUES ({placeholders});
    """

    total_rows = len(df)
    print(f"Attempting to insert {total_rows} rows into {table_name}.")

    try:
        with psycopg2.connect(
            host=host_name,
            dbname=db_name,
            user=user_name,
            password=pw
        ) as conn:
            conn.autocommit = False

            with conn.cursor() as cur:
                rows_inserted = 0

                for idx, (df_idx, row) in enumerate(df.iterrows(), start=1):
                    row_dict = row.to_dict()

                    try:
                        cur.execute(insert_sql, row_dict)
                        rows_inserted += 1
                    except Exception as e:
                        print("\nERROR inserting row:")
                        print(f"  DataFrame index: {df_idx}")
                        if "unitid" in row_dict:
                            print(f"  unitid: {row_dict['unitid']}")
                        if "INSTNM" in row_dict:
                            print(f"  INSTNM: {row_dict['INSTNM']}")
                        print(f"  Full row: {row_dict}")
                        print(f"  Postgres error: {e}")

                        conn.rollback()
                        print("\nROLLBACK COMPLETE — no rows inserted.")
                        print(f"Attempted to insert {rows_inserted} of {total_rows} rows before failure.")
                        return

                conn.commit()
                print("\nTRANSACTION COMMITTED.")
                print(f"{total_rows} rows inserted successfully.")

    except Exception as e:
        print("Connection or top-level error:")
        print(e)


insert_dataframe_strict(
    df=institution_financial_df,
    table_name="institution_financial",
    host_name=host,
    db_name=dbname,
    user_name=username,
    pw=password
)
insert_dataframe_strict(
    df=institution_scorecard_info_df,
    table_name="institution_scorecard_info",
    host_name=host,
    db_name=dbname,
    user_name=username,
    pw=password
)
insert_dataframe_strict(
    df=institution_admissions_df,
    table_name="institution_admissions",
    host_name=host,
    db_name=dbname,
    user_name=username,
    pw=password
)
insert_dataframe_strict(
    df=institution_completion_df,
    table_name="institution_completion",
    host_name=host,
    db_name=dbname,
    user_name=username,
    pw=password
)

conn.close()
