import sqlite3
from sqlite3.dbapi2 import OperationalError

def load_into_database(dataframe, conn):
    try:
        for table_name, df in [('Region', dataframe[['Region_Name']].drop_duplicates()),
                               ('BIG', dataframe[['BIG_Name']].drop_duplicates()),
                               ('Region_BIG', dataframe[['Region_ID', 'BIG_ID']].drop_duplicates())]:
            df.to_sql(table_name, conn, if_exists='replace', index=False)
        for prefix in ['FT', 'PT', 'FTPT', 'All']:
            df = dataframe[[f'{prefix}_Public', f'{prefix}_Private', f'{prefix}_Pub_Priv']]
            df.to_sql(f'{prefix}_Employees', conn, if_exists='replace', index=False)
    except OperationalError as e:
        print("Error in database loading:", str(e))
