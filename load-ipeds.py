import sys
import pandas as pd
import numpy as np
import psycopg2
import credentials_copy
from sqlalchemy import create_engine
from insert_dataframe import insert_dataframe
import importlib.util

file_name = sys.argv[1]
ipeds_df = pd.read_csv(file_name, encoding = 'latin1')
ipeds_df['YEAR'] = int(file_name[-8:-4])

institution_ipeds_info_df = ipeds_df[['UNITID', 'INSTNM', 'ADDR','CITY','STABBR','ZIP','FIPS',
                                      'COUNTYCD','COUNTYNM','CBSA','CBSATYPE','CSA','LATITUDE','LONGITUD',
                                      'CCBASIC', 'YEAR']]

if "CCBASIC" in institution_ipeds_info_df.columns:
    institution_ipeds_info_df["CCBASIC"] = pd.to_numeric(
        institution_ipeds_info_df["CCBASIC"], errors="coerce"
    )

    institution_ipeds_info_df["CCBASIC"] = institution_ipeds_info_df["CCBASIC"].apply(
        lambda x: None if pd.isna(x) or x < 0 or x > 33 else int(x)
    )

institution_ipeds_info_df = institution_ipeds_info_df.replace({pd.NA: None, np.nan: None})

username = credentials_copy.DB_USER
database = credentials_copy.DB_USER
password = credentials_copy.DB_PASSWORD
db_url = f"postgresql://" + username + ":" + password + "@debprodserver.postgres.database.azure.com:5432/" + database
engine = create_engine(db_url)

host = "debprodserver.postgres.database.azure.com"

insert_dataframe(institution_ipeds_info_df, "institution_ipeds_info",
                 host, database, username, password)
