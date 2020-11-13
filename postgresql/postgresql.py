import psycopg2
import json
from config import login
from connection_check import conn_check as wrapper


class Postgresql:
    def __init__(self, database=None, user=None, password=None, host=None, port=None):
        self.essentials = locals()
        self.essentials.pop('self')
        self.data = None
        self.conn = None
        self.cursor = None

# Executes given query.
    @wrapper
    def __call__(self, query):
        try:
            self.cursor.execute(query)
            self.conn.commit()
            print('successful query:', query)

        # Todo: Test if this part can be taken away.
        except psycopg2.errors.InFailedSqlTransaction:
            self.close_conn()
            self.connect()
        except Exception as e:
            print(e)

    # Activates "with" keyword. This will keep the connection open.
    def __enter__(self):
        self.connect()
        return self

    # Exits out of with statement.
    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type:
            print('exc_type:', exc_type)
            print('exc_traceback:', exc_tb)
            print('exc_value:', exc_val)
            self.conn.close()
            self.connect()
            return
        self.close_conn()
        return True

    # Attempts to connect to database. Can give other credentials than listed.
    def connect(self, **kwargs):
        if not kwargs:
            kwargs = self.essentials
        try:
            self.conn = psycopg2.connect(**kwargs)
            self.cursor = self.conn.cursor()
            print('Connected to database')
        except Exception as e:
            print(e, type(e))
            self.close_conn()

    # Closes connection.
    def close_conn(self):
        try:
            self.conn.close()
            self.conn = None
            print('Connection closed')
        except Exception as e:
            print(e, type(e))

    # Creates and executes query to create table.
    def create_table(self, table, **kwargs):
        insertion = f"{', '.join([f'{key} {value}' for key, value in kwargs.items()])}"
        self(f"CREATE TABLE IF NOT EXISTS {table} ({insertion})")

    # Creates and executes query to insert into table.
    def insert(self, table, **kwargs):
        join_keys = ", ".join(str(key) for key in kwargs.keys())
        join_values = ", ".join(f"'{value}'" for value in kwargs.values())
        self(f"INSERT INTO {table} ({join_keys}) VALUES ({join_values})")

    # This may not be the functional version. Will check this later.
    # Creates and executes query for inserting json (dictionary) object.
    def insert_json(self, table, column, **kwargs):
        self(f"INSERT INTO {table} ({column}) VALUES ('{json.dumps(kwargs)}')")

    # Fetches data from given query.
    # Returns fetched data.
    @wrapper
    def _fetch(self, query):
        self.cursor.execute(query)
        data = self.cursor.fetchall()
        return data

    # Creates and executes query to fetch all elements from given table.
    # Returns fetches data.
    def fetchall(self, table):
        return self._fetch(f'SELECT * FROM {table}')

    # Creates and executes query to fetch given elements from given table.
    # *args: Items to select from table.
    # **kwargs: Object identifier and value.
    # Returns: Fetched data as list.
    def fetch(self, table, *args, **kwargs):
        if args:
            args = f'({", ".join(args)})'
        else:
            args = '*'
        data = []
        base = f'SELECT {args} FROM {table} WHERE'
        print(base)
        for item, value in kwargs.items():
            data.append(self._fetch(f'{base} {item} = {value}'))
        return data

