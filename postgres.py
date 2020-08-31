import pandas as pd
import psycopg2

config = {
    'database': '',  # Insert name of database
    'user': '',  # Insert db username
    'password': '',  # Insert db password
    'host': '',  # Insert db host (ip)
    'port': ''  # Insert port for db (default 5432 for postgresql)
}


class Postgres:

    def __init__(self, database=None, user=None, password=None, host=None, port=None):
        self.essentials = locals()
        self.essentials.pop('self')
        self.conn = None
        self.cursor = None

    def connect(self):
        try:
            self.conn = psycopg2.connect(**self.essentials)
            self.cursor = self.conn.cursor()
        except Exception as e:
            print(e, type(e))

    def commit(self, query):
        try:
            self.connect()
            self.cursor.execute(query)
            self.conn.commit()
        except Exception as e:
            print(e, type(e))
        finally:
            self.conn.close()

    def fetch(self, query):
        data = None
        try:
            self.connect()
            data = self.cursor.execute(query)
        except Exception as e:
            print(e, type(e))
        finally:
            self.conn.close()
            return data

    def create_table(self, table, **kwargs):
        self.commit("CREATE TABLE %s ("
                    % table + ", ".join(list(str(key) + ' ' + str(value) for key, value in kwargs.items())) + ")"
                    )

    def insert(self, table, **kwargs):
        joinkeys = lambda **dictionary: ", ".join(str(key) for key in dictionary.keys())
        joinvalues = lambda **dictionary: ", ".join('"' + str(key) + '"' for key in dictionary.values())
        self.commit("INSERT INTO %s (%s) VALUES (%s)" % (table, joinkeys(**kwargs), joinvalues(**kwargs)))

    def fetchone(self, table, **kwargs):
        pass

    def fetchall(self, table):
        return self.fetch("SELECT * FROM %s" % table)

    def update(self):
        pass

    def delete(self):
        pass

    def create_db(self):
        pass

    def dataframe(self, table):
        return pd.DataFrame(data=self.fetchall(table))


sql_session = Postgres(**config)
print(sql_session.essentials)
