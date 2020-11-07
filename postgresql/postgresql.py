import psycopg2
# import pandas as pd (currently not working.)
import json


class Postgresql:
    def __init__(self, database=None, user=None, password=None, host=None, port=None):
        self.essentials = locals()
        self.essentials.pop('self')
        self.data = None
        self.conn = None
        self.cursor = None

    def __call__(self, query):
        try:
            self.cursor.execute(query)
            self.conn.commit()
            print('successful query:', query)
        except psycopg2.errors.InFailedSqlTransaction:
            self.conn.close()
            self.conn = psycopg2.connect(**self.essentials)
            self.cursor = self.conn.cursor()
        except Exception as e:
            print(e)

    def __try(self, function, data=None, *args, **kwargs):
        try:
            data = function(*args, **kwargs)
        except Exception as e:
            print(e, type(e))
        finally:
            return data

    def __enter__(self):
        self.conn = psycopg2.connect(**self.essentials)
        self.cursor = self.conn.cursor()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        try:
            if exc_type:
                print('exc_type:', exc_type)
                print('exc_traceback:', exc_tb)
                print('exc_value:', exc_val)
                self.conn.close()
                self.__enter__()
        except:
            print('hello')
            self.conn.close()
            return True

    def commit(self, query):
        self.__try(self.__execute, query=query)
        self.conn.commit()
        self.conn.close()

    def create_table(self, table, **kwargs):
        insertion = f"{', '.join([f'{key} {value}' for key, value in kwargs.items()])}"
        return f"CREATE TABLE IF NOT EXISTS {table} ({insertion})"

    def insert(self, table, **kwargs):
        join_keys = ", ".join(str(key) for key in kwargs.keys())
        join_values = ", ".join(f"'{value}'" for value in kwargs.values())
        return f"INSERT INTO {table} ({join_keys}) VALUES ({join_values})"

    def insert_json(self, table, column, **kwargs):
        return f"INSERT INTO {table} ({column}) VALUES ('{json.dumps(kwargs)}')"

    def fetchone(self, table, **kwargs):
        pass

    def fetchall(self, table):
        self.data = self.fetch(f"SELECT * FROM {table}")

    def update(self):
        pass

    def delete(self):
        pass

    def create_db(self):
        pass

    def dataframe(self, table):
        self.fetchall(table)
        df = pd.DataFrame(data=self.data)
        return df



# # # # Enter your personal on information here
login = {
    'database': '',
    'user':     '',
    'password': '',
    'host':     '',
    'port':     ''
}


# Example of how to initialize the class.
# pg = Postgresql(**login)



# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# Some of the functions are not working yet. Feel free to help out with better solutions! :)#
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
