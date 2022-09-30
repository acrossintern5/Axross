from dotenv import load_dotenv
import os
from os.path import join
import os
import psycopg2
import pandas as pd
from pandas import DataFrame
import pandasql
import pygsheets
from datetime import date, datetime, timedelta
from pyparsing import col
import pytz


class extractedHeroku():
    def connection_heroku(query, database2, host2, port2, user2, password2):
        try:
            connection = psycopg2.connect(database=database2,
                                          host=host2,
                                          port=port2,
                                          user=user2,
                                          password=password2)
            cursor = connection.cursor()
            cursor_query = query
            cursor.execute(cursor_query)
            df_db = cursor.fetchall()
            cols = []
            for col in cursor.description:
                cols.append(col[0])
            df = pd.DataFrame(data=df_db, columns=cols)
            print("Total rows are:  ", len(df_db))

        except (Exception, psycopg2.Error) as error:
            print("Error while fetching data from PostgreSQL", error)

        finally:
            # closing database connection.
            if connection:
                cursor.close()
                connection.close()
                print("PostgreSQL connection is closed")
        return df

    def import_to_sheet(service_file3, df_extract):
        gc = pygsheets.authorize(
            service_file=service_file3)
        sheet = gc.open_by_key('1Cc1RpoV1JQzxTMfTJRjYedUN0msye2O061SvZU-ida4')
        wks = sheet.worksheet_by_title('test')
        wks.rows = df_extract.shape[0]
        wks.set_dataframe(df_extract, (1, 1))


# Initialize dotenv path
dotenv_path = join(r'D:\Axross\heroku_extract\env')
load_dotenv(dotenv_path)

# Initialize all the credential
database3 = os.environ.get('DATABASE')
host3 = os.environ.get('HOST_NAME')
port3 = os.environ.get('PORT_NUMBER')
user3 = os.environ.get('USER_NAME')
password3 = os.environ.get('PASSWORD')
service_file3 = os.environ.get('SERVICE_FILE')


sql_query = '''select * from merchants'''

df_extract = extractedHeroku.connection_heroku(
    sql_query, database3, host3, port3, user3, password3)
import_sheet = extractedHeroku.import_to_sheet(service_file3, df_extract)
