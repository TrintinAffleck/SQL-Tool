import pandas as pd
from sqlalchemy import create_engine
from urllib.parse import quote_plus
from Reading_Script import *

'''Inputs/Variables'''                    
server = input("Server Name:")
database = input("Database Name:")
auths = input("Authentication:")
trusted_conn: bool = False
filenameinput = input ("FileName:")

results = any
cols = any

class Sqlclass:
    def Main(self) -> pd.DataFrame:
        if len(filenameinput) > 0:
            try:
                self.SqlConnect(auths)
                excelfile = self.ExecuteSQLQuery(query,False)
                excelfile.to_excel(f'{filenameinput}.xlsx', 'HealthCheckSheet', index=False)
            except Exception:
                print("Could not connect to database or execute query.")
                print("Check you've entered the right credentials.")
        elif len(filenameinput) <= 0:
            print("FileName is invalid")
        return pd.DataFrame
        

    def SqlConnect(self, auths: str):
        auth_list = ('Windows'
         ,'Windows Authentication'
         ,'Trusted Connection'
         ,'windows'
         ,'windows authentication'
         ,'trusted connection')
        if auths in auth_list:
            trusted_conn = True
            params = quote_plus(
            'Driver={SQL Server};'
            f'Server={server};'
            f'Database={database};'
            'Trusted_Connection=yes' + ';'
            'Autocommit=True'
            )
            #Creating the connection string using SQL Alchemy
            self.engine = create_engine('mssql+pyodbc:///?odbc_connect=%s' % params) 
            print("Connecting with trusted connection...")
        else:
            if (not trusted_conn):
                uid = input("Username:")
                pwd = input("Password:")
            params = quote_plus(
            'Driver={SQL Server};'
            f'Server={server};'
            f'Database={database};'
            f'UID={uid};'
            f'PWD={pwd};'
            'Trusted_Connection=no' + ';'
            'Autocommit=True'
            )
            self.engine = create_engine('mssql+pyodbc:///?odbc_connect=%s' % params)
            print("Connecting with credentials...")
        
        return params

    def ExecuteSQLQuery(self, 
                        query: str, 
                        is_proc: bool = False,
                        param: tuple = None) -> pd.DataFrame: 
        """Generic Query and Proc executor."""
        if not is_proc:
            connection = self.engine.raw_connection()
            cols = []
            results = []
            try:
                cursor = connection.cursor()
                cursor.execute(query)
                not_end = True
                while not_end:
                    try:
                        results = [tuple(row) for row in cursor.fetchall()]
                        cols = [t[0] for t in cursor.description]
                    except Exception:
                        pass
                    not_end = cursor.nextset()
                cursor.close()
            finally:
                connection.close()
            output = pd.DataFrame(results, columns=cols)
            return output

        with self.engine.connect() as connection:
            with connection.begin() as transaction:
                output = pd.read_sql_query(query, 
                                            connection, 
                                            params=param)
                transaction.commit()
                return output
                

'''Create Object'''
myobject = Sqlclass

'''Function Calls'''
myobject.Main()

