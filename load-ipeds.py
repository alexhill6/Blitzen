import sys
import pandas as pd
import numpy as np
import psycopg2
import credentials_copy


file_name = sys.argv[1]
ipeds_df = pd.read_csv(file_name, encoding = 'latin1')
ipeds_df['YEAR'] = int(file_name[-8:-4])

institution_ipeds_info_df = ipeds_df[['UNITID', 'INSTNM', 'ADDR','CITY','STABBR','ZIP','FIPS',
                                      'COUNTYCD','COUNTYNM','CBSA','CBSATYPE','CSA','LATITUDE','LONGITUD',
                                      'CCBASIC', 'YEAR']]
institution_ipeds_info_df = institution_ipeds_info_df.replace({pd.NA: None, np.nan: None})

from sqlalchemy import create_engine
import importlib.util
import credentials_copy
username = "elliehua"
database = "elliehua"
password = credentials_copy.DB_PASSWORD
db_url = f"postgresql://" + username + ":" + password + "@debprodserver.postgres.database.azure.com:5432/" + database
engine = create_engine(db_url)

# insert_dataframe.py
def insert_dataframe(df, table_name, host_name, db_name, user_name, pw):
    """
    Inserts all rows of a DataFrame into a SQL table using parameterized
    prepared statements. Rolls back if any row fails.
    """

    # Create column list: ("col1", "col2", ...)
    columns = list(df.columns)
    col_names = ", ".join(columns)
    placeholders = ", ".join([f"%({c})s" for c in columns])  # named parameters

    # Build the "update" part of the ON CONFLICT clause
    update_assignments = ", ".join([f"{c} = EXCLUDED.{c}" for c in columns if c != "unitid"])

    insert_sql = f"""
        INSERT INTO {table_name} ({col_names})
        VALUES ({placeholders})
        ON CONFLICT (unitid)
        DO UPDATE SET {update_assignments};
    """
    fail_count = 0
    total_read = 0
    try:
        with psycopg2.connect(host=host_name, dbname=db_name, user=user_name, password=pw) as conn:
            with conn.cursor() as cur:
                for _, row in df.iterrows():
                    # Convert Series → dict for named placeholders
                    row_dict = row.to_dict()
                    try:
                        cur.execute(insert_sql, row_dict)
                    except psycopg2.errors.ForeignKeyViolation as e:
                        print(f"Foreign key violation on row: {row_dict}")
                        print(e)
                        conn.rollback()
                        fail_count += 1
                    except Exception as e:
                        print(f"Error inserting row: {row_dict}")
                        print(e)
                        conn.rollback()
                        fail_count += 1                 
                    total_read += 1
                conn.commit()
                print(f"{total_read} rows read successfully.")
                print(f"{total_read - fail_count} rows loaded successfully.")
                print(f"{fail_count} rows unsuccessfully loaded.")
    except Exception as e:
        print("Connection or execution error:")
        print(e)
host="debprodserver.postgres.database.azure.com"
dbname=credentials_copy.DB_USER
user=credentials_copy.DB_USER
password=credentials_copy.DB_PASSWORD

insert_dataframe(institution_ipeds_info_df, "institution_ipeds_info", host, dbname, user, password)