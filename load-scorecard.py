import sys
import pandas as pd
import psycopg2
import numpy as np
from sqlalchemy import create_engine
import importlib.util
import credentials_copy
from insert_dataframe import insert_dataframe

# Read in command line argument (csv file to be loaded)
scorecard_df = pd.read_csv(sys.argv[1])

# Calculate YEAR (e.g., if filename ends in 2023.csv, year is 2024)
year = int(sys.argv[1][-14:-10]) + 1 

scorecard_df['YEAR'] = year

# df column selection
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

# Replace missing values with NaN/NA
dataframes_to_clean = [
    institution_financial_df, institution_scorecard_info_df,
    institution_admissions_df, institution_completion_df,
]

for df in dataframes_to_clean:
    df.replace({pd.NA: None, np.nan: None}, inplace=True)

# Connect to database
username = credentials_copy.DB_USER
database = credentials_copy.DB_USER
password = credentials_copy.DB_PASSWORD
db_url = f"postgresql://" + username + ":" + password + "@debprodserver.postgres.database.azure.com:5432/" + database
engine = create_engine(db_url)

if "UNITID" not in scorecard_df.columns:
    raise ValueError("DataFrame is missing 'unitid' column")

# Get valid unitids from IPEDS parent table
host = "debprodserver.postgres.database.azure.com"
dbname = database
user = username

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

# Filter to only rows whose unitid exists in IPEDS
institution_financial_df = institution_financial_df[institution_financial_df["UNITID"].isin(valid_unitid_set)].copy()
institution_scorecard_info_df = institution_scorecard_info_df[institution_scorecard_info_df["UNITID"].isin(valid_unitid_set)].copy()
institution_admissions_df = institution_admissions_df[institution_admissions_df["UNITID"].isin(valid_unitid_set)].copy()
institution_completion_df = institution_completion_df[institution_completion_df["UNITID"].isin(valid_unitid_set)].copy()

kept = len(institution_financial_df)
total_read = len(scorecard_df) # Use original length to calculate dropped rows
dropped = total_read - kept

print(f"{kept} rows have matching unitid in institution_ipeds_info.")
print(f"{dropped} rows dropped (no matching unitid in IPEDS).")

# Insert into dataframe
insert_dataframe(
    df=institution_financial_df,
    table_name="institution_financial",
    host_name=host,
    db_name=dbname,
    user_name=user,
    pw=password
)
insert_dataframe(
    df=institution_scorecard_info_df,
    table_name="institution_scorecard_info", 
    host_name=host,
    db_name=dbname,
    user_name=user,
    pw=password
)
insert_dataframe(
    df=institution_admissions_df,
    table_name="institution_admissions",
    host_name=host,
    db_name=dbname,
    user_name=user,
    pw=password
)
insert_dataframe(
    df=institution_completion_df,
    table_name="institution_completion",
    host_name=host,
    db_name=dbname,
    user_name=user,
    pw=password
)
