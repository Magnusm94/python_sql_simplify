import pandas as pd
import psycopg2


class Postgresql:

    def __init__(self, use_config=True, database=None, user=None, password=None, host=None, port=None):
        """
        Creates class variables. If you fill config.py, you will not need to give any parameters.
        :param use_config: Bool of whether or not to use config file.
        :param database: Name of database.
        :param user: Username for database.
        :param password: Password for database.
        :param host: Hostname (domain or ip).
        :param port: Connection port.
        """
        if use_config:
            from postgresql import config as config
            self.essentials = config
        else:
            self.essentials = locals()
            self.essentials.pop('self')
        self.conn = None
        self.cursor = None

    def __connect(self):
        """
        Creates a connection to the database, but does not close it. This function should not be run standalone.
        :return: None.
        """
        try:
            self.conn = psycopg2.connect(**self.essentials)
            self.cursor = self.conn.cursor()
        except Exception as e:
            print(e, type(e))

    def commit(self, query):
        """
        Starts a connection to the database, commits given query, and closes connection.
        :param query: SQL query.
        :return: None.
        """
        try:
            self.__connect()
            self.cursor.execute(query)
            self.conn.commit()
        except Exception as e:
            print(e, type(e))
        finally:
            self.conn.close()

    def fetch(self, query):
        """
        Creates connection, fetches given query, closes connection and returns collected data.
        This function does not commit any query, and should therefore not be used for anything besides fetching data.
        :param query: SQL query.
        :return: Data from given fetch query.
        """
        data = None
        try:
            self.__connect()
            data = self.cursor.execute(query)
        except Exception as e:
            print(e, type(e))
        finally:
            self.conn.close()
            return data

    def create_table(self, table, **kwargs):
        """
        Creates and commits a query to create a table.
        :param table: Name of table.
        :param kwargs: Name of data in tables = datatype. | Example name='text', age='int'.
        :return: None.
        """
        self.commit("CREATE TABLE IF NOT EXISTS %s ("
                    % table + ", ".join(list(str(key) + ' ' + str(value) for key, value in kwargs.items())) + ")"
                    )

    def insert(self, table, **kwargs):
        """
        Creates and commits a query for insertion to a given table.
        :param table: Name of table.
        :param kwargs: Name of column = value to insert. | Example: firstname='foo', lastname='bar'.
        :return: None.
        """
        joinkeys = lambda **dictionary: ", ".join(str(key) for key in dictionary.keys())
        joinvalues = lambda **dictionary: ", ".join("'" + str(value) + "'" for value in dictionary.values())
        self.commit("INSERT INTO %s (%s) VALUES (%s)" % (table, joinkeys(**kwargs), joinvalues(**kwargs)))

    def fetchone(self, table, **kwargs):
        pass

    def fetchall(self, table):
        """
        Creates a fetch query and returns data.
        :param table: Name of table.
        :return: All data from given table.
        """
        return self.fetch("SELECT * FROM %s" % table)

    def update(self):
        pass

    def delete(self):
        pass

    def create_db(self):
        pass

    def dataframe(self, table):
        """
        Creates and returns a pandas dataframe of a table.
        :param table: Name of table.
        :return: Dataframe of entire table.
        """
        return pd.DataFrame(data=self.fetchall(table))


# Creating a session using config dictionary:
# postgres_session = Postgresql(**config)
