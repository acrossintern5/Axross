import os
import psycopg2
import pandas as pd
from pandas import DataFrame
import pandasql
import pygsheets
from datetime import date, datetime, timedelta
import pytz
from sqlalchemy import null
from dotenv import load_dotenv
import os
from os.path import join


class HerokuPostgres():
    def __init__(self, database2, host2, port2, user2, password2):
        try:
            self.connection = psycopg2.connect(database=database2,
                                               host=host2,
                                               port=port2,
                                               user=user2,
                                               password=password2)
        except Exception as e:
            print(e)
        else:
            print('Psycorg / Postgresql connection success')
            print('')

    def psycorg_conn(self):
        return self.connection

    def method1_extract(self, query):
        '''Query full data from Postgres'''
        try:
            global df
            connection = self.psycorg_conn()
            cursor = connection.cursor()
            cursor_query = query
            cursor.execute(cursor_query)
            df_db = cursor.fetchall()
            cols = []
            for col in cursor.description:
                cols.append(col[0])
            df = pd.DataFrame(data=df_db, columns=cols)
        except (Exception, psycopg2.Error) as error:
            print("Error while fetching data from PostgreSQL", error)
        finally:
            if connection:
                cursor.close()
                connection.close()
                print("Method 1 : Extration done. PostgreSQL connection is closed")
                print(f'Total rows are:  {len(df.index)}')
        return(df)


class HerokuGoogleSheet():
    def __init__(self, service_file, sheet_id, tab_title):
        self.service_file = service_file
        self.sheet_id = sheet_id
        self.tab_title = tab_title

        try:
            self.gc = pygsheets.authorize(service_file=self.service_file)
            self.sheet_id_con = self.gc.open_by_key(self.sheet_id)
            self.tab_title_con = self.sheet_id_con.worksheet_by_title(
                self.tab_title)
        except Exception as e:
            print(e)
        else:
            print('Google sheet connection success')
            print('')

    def pygsheets_con(self):
        return self.gc

    def call_sheet_id(self):
        return self.sheet_id_con

    def call_tab_title(self):
        return self.tab_title_con

    def dump_import(self, df_Import):
        wks = self.call_tab_title()
        wks.rows = df_Import.shape[0]
        wks.set_dataframe(df_Import, (1, 1))


dotenv_path = join(r'D:\Axross\heroku_extract\env')
load_dotenv(dotenv_path)

# Step 2: Get Credential from .env (Make sure the name inside "get" same with .env)
database3 = os.environ.get('DATABASE')
host3 = os.environ.get('HOST_NAME')
port3 = os.environ.get('PORT_NUMBER')
user3 = os.environ.get('USER_NAME')
password3 = os.environ.get('PASSWORD')
service_file3 = os.environ.get('SERVICE_FILE')
sheet_id3 = os.environ.get('SHEET_ID')
tab_title3 = os.environ.get('TAB_TITLE')

googlesheet = HerokuGoogleSheet(
    service_file=service_file3,
    sheet_id=sheet_id3,
    tab_title=tab_title3)

postgres = HerokuPostgres(database2=database3,
                          host2=host3,
                          port2=port3,
                          user2=user3,
                          password2=password3)

sql_query = '''select * from merchants'''


try:
    # Step 6: Initialize GSheet link to open sheet
    googlesheet.call_sheet_id()
    try:
        # Step 7: Read Sheet and select sheet title
        wks_df = googlesheet.call_tab_title().get_as_df(include_tailing_empty=False)
        full_df = postgres.method1_extract(sql_query)
        googlesheet.dump_import(full_df)

    except:
        print('Error : Recheck assigned worksheet name')
except:
    print('Error : Recheck Sheet ID')
