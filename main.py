import datetime
import pandas as pd
import clickhouse_connect
from sqlalchemy import text
from db.db_connector import DBConnector
from common.utils import time_this

db = DBConnector()

# def fix_old_dates(df, datetime_columns):
#     """Fix old and NULL datetime values for ClickHouse compatibility."""
#     def adjust_year(date):
#         if pd.isnull(date) or date.year < 1970:
#             return datetime.datetime(1970, 1, 1, 0, 0, 0)  
#         return datetime.datetime(date.year, date.month, date.day, 0, 0, 0)  

#     for col in datetime_columns:
#         df[col] = pd.to_datetime(df[col], errors='coerce')  
#         df[col] = df[col].apply(adjust_year)  
#     return df

def char_to_varchar(column, df):
    for col in column:
        try:
            df[col] = df[col].astype(str).str.strip()
        except Exception as e:
            print(e)
    return df

@time_this
def main(r):
    ms = db.source()
    cl = db.clickhouse()
    
    with ms.connect() as conn:
        conn.autocommit = True
        df = pd.read_sql('SELECT * FROM testtb', conn)
        
        col = pd.read_sql(
            "SELECT COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = 'testtb'", conn
        )
        
        column = col[col['DATA_TYPE'].str.contains('char', case=False)]['COLUMN_NAME'].tolist()
        
        print(column)
        print('before')
        print(df)
        
        df = char_to_varchar(column, df)
        
        print('after')
        print(df)
        # cl.insert('testtb', df, column_names=df.columns.tolist())
        print('pushed into click')

        conn.commit()

        with conn.execute(text("SELECT COUNT(*) FROM testtb")) as res:
            res = res.fetchone()
        
    return res


if __name__ == "__main__":
    record = {
        'sourceid': 'source_identifier',
        'targetobject': 'target_object'
    }
    main(record)

# datetime_columns = col[col["DATA_TYPE"].str.contains("date|datetime", case=False)]["COLUMN_NAME"].tolist()

# df = fix_old_dates(df, datetime_columns)  
# data = [tuple(row) for row in df.itertuples(index=False, name=None)]
# cl.insert('date_col', data, column_names=df.columns.tolist())
# print("success")


# def fix_old_dates(df, datetime_columns):
#     """Fix old and NULL datetime values for ClickHouse compatibility."""
#     def adjust_year(date):
#         if pd.isnull(date) or date.year < 1970:
#             return datetime.datetime(1970, date.month, date.day)  
#         return datetime.datetime(date.year, date.month, date.day)  

#     for col in datetime_columns:
#         df[col] = pd.to_datetime(df[col], errors='coerce')  
#         df[col] = df[col].apply(adjust_year)  
#     return df

# ms =  db.source() # returns sql server engine
# cl = clickhouse_connect.get_client(
#     host="localhost", 
#     port=8123, 
#     username="admin", 
#     password="admin",
#     database="my_database"
# )

# df = pd.read_sql('SELECT * FROM date_col1', ms)
# print(df)
# col = pd.read_sql('SELECT COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = \'date_col1\'', ms)
# datetime_columns = col[col["DATA_TYPE"].str.contains("datetime", case=False)]["COLUMN_NAME"].tolist()


# df = fix_old_dates(df, datetime_columns) 
# data = [tuple(row) for row in df.itertuples(index=False, name=None)]
# cl.insert('date_col', data, column_names=df.columns.tolist())
