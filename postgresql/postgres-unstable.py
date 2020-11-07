# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# This file is not tested at all since last update, but is my latest progress #
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

import psycopg2
import pandas as pd
import json



class Postgresql:
    def __init__(self, database=None, user=None, password=None, host=None, port=None):
        self.essentials = locals()
        self.essentials.pop('self')
        self.data = None
        self.conn = None
        self.cursor = None

# # Have had some issues with this one, but not in it's current state. 
# # The issues were related to psycopg2.errors.InFailedSqlTransaction which stopped all further queries until new connection was made.
# # May or may work like this, would not use like this if it was important not to get any data loss.
    def __call__(self, query):
        try:
            self.cursor.execute(query)
            self.conn.commit()
            print('successful query:', query)
            return True
        except psycopg2.errors.InFailedSqlTransaction:
            self.conn.close()
            self.conn = psycopg2.connect(**self.essentials)
            self.cursor = self.conn.cursor()
        except Exception as e:
            print(e)

# # Working on replacing this function completely.
    def __try(self, function, data=None, *args, **kwargs):
        try:
            data = function(*args, **kwargs)
        except Exception as e:
            print(e, type(e))
        finally:
            return data

# # This function appears to work fine like this.
    def __enter__(self):
        self.conn = psycopg2.connect(**self.essentials)
        self.cursor = self.conn.cursor()
        return self

# # This method is not perfect as is. Needs tweaking.
    def __exit__(self, exc_type, exc_val, exc_tb):
        try:
            if exc_type:
                print('exc_type:', exc_type)
                print('exc_traceback:', exc_tb)
                print('exc_value:', exc_val)
                self.conn.close()
                self.__enter__()
        except:
            self.conn.close()
            return True

# # Not tested
    def try_query(self, query):
        try:
            if self(query) == True:
                return True
            else:
                return query
        except Exception as e:
            print(e, type(e))

# # Currently not working.
    def commit(self, query):
        self.__try(self.__execute, query=query)
        self.conn.commit()
        self.conn.close()

# # Not tested yet. Queries are fine, but unsure of the redirection through self.try_query
    def create_table(self, table, **kwargs):
        insertion = f"{', '.join([f'{key} {value}' for key, value in kwargs.items()])}"
        return self.try_query(f"CREATE TABLE IF NOT EXISTS {table} ({insertion})")

# # Not tested yet. Queries are fine, but unsure of the redirection through self.try_query
    def insert(self, table, **kwargs):
        join_keys = ", ".join(str(key) for key in kwargs.keys())
        join_values = ", ".join(f"'{value}'" for value in kwargs.values())
        return self.try_query(f"INSERT INTO {table} ({join_keys}) VALUES ({join_values})")

# # Not tested yet. Queries are fine, but unsure of the redirection through self.try_query
    def insert_json(self, table, **kwargs):
        kwargs_keys = []
        kwargs_values = []
        for key, value in kwargs.items():
            kwargs.keys.append(key)
            if type(v) == dict:
                kwargs_values.append(f"'{json.dumps(v)}'")
            else:
                kwargs_values.append(str(v))
        return self.try_query(f"INSERT INTO {table} ({', '.join(kwargs_keys)}) VALUES ({', '.join(kwargs_values)})")

# # Todo: Create this function
    def fetchone(self, table, **kwargs):
        pass

# # Todo: Fix this function
    def fetchall(self, table):
        self.data = self.fetch(f"SELECT * FROM {table}")

# # Todo: Create this function
    def update(self):
        pass

# # Todo: Create this function
    def delete(self):
        pass

# # Todo: Create this function
    def create_db(self):
        pass

# # Currently not working
    def dataframe(self, table):
        self.fetchall(table)
        df = pd.DataFrame(data=self.data)
        return df


login = {
    'database': '',
    'user':     '',
    'password': '',
    'host':     '',
    'port':     ''
}
