import psycopg2
import pandas as pd


class Postgresql:
    def __init__(self, database=None, user=None, password=None, host=None, port=None):
        self.essentials = locals()
        self.essentials.pop('self')
        self.data = None
        self.conn = None
        self.cursor = None

    def __try(self, function, data=None, *args, **kwargs):
        try:
            data = function(*args, **kwargs)
        except Exception as e:
            print(e, type(e))
        finally:
            return data

    def __connect(self):
        self.conn = psycopg2.connect(**self.essentials)
        self.cursor = self.conn.cursor()

    def __execute(self, query):
        self.__connect()
        data = self.cursor.execute(query)
        return data

    def connect(self):
        self.__try(self.__connect)

    def commit(self, query):
        self.__try(self.__execute, query=query)
        self.conn.commit()
        self.conn.close()

    def fetch(self, query):
        data = self.__try(self.__execute, query=query)
        self.conn.close()
        return data

    def create_table(self, table, **kwargs):
        insertion = f"{', '.join([f'{key} {value}' for key, value in kwargs.items()])}"
        self.commit(f"CREATE TABLE IF NOT EXISTS {table} ({insertion})")

    def insert(self, table, **kwargs):
        join_keys = ", ".join(str(key) for key in kwargs.keys())
        join_values = ", ".join(f"'{value}'" for value in kwargs.values())
        self.commit(f"INSERT INTO {table} ({join_keys}) VALUES ({join_values})")

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