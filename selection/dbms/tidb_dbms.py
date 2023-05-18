import logging
import re
import pymysql

from selection.database_connector import DatabaseConnector

class TiDBDatabaseConnector(DatabaseConnector):
    def __init__(self, db_name, autocommit=False):
        DatabaseConnector.__init__(self, db_name, autocommit=autocommit)
        self.db_system = "TiDB"
        self._connection = None

        if not self.db_name:
            self.db_name = "test"
        self.create_connection()

        self.set_random_seed()
        logging.debug("TiDB connector created: {}".format(db_name))

    def create_connection(self):
        if self._connection:
            self.close()
        self._connection = pymysql.connect(host='127.0.0.1',
                     user='root',
                     password='',
                     database='test')
        self._cursor = self._connection.cursor()
    
    def enable_simulation(self):
        pass # Do nothing

    def database_names(self):
        result = self.exec_fetch("show databases", False)
        return [x[0] for x in result]
    
    def update_query_text(self, text):
        pass # TODO

    def _add_alias_subquery(self, query_text):
        pass # TODO

    def create_database(self, database_name):
        self.exec_only("create database {}".format(database_name))
        logging.info("Database {} created".format(database_name))


    def import_data(self, table, path, delimiter="|"):
        pass # TODO

    def indexes_size(self):
        # TODO
        return 0

    def drop_database(self, database_name):
        statement = f"DROP DATABASE {database_name};"
        self.exec_only(statement)

        logging.info(f"Database {database_name} dropped")

    def create_statistics(self):
        logging.info("TiDB: Run `analyze`")
        self.commit()
        self._connection.autocommit = True
        self.exec_only("analyze")
        self._connection.autocommit = self.autocommit

    def set_random_seed(self, value=0.17):
        pass # TODO

    def supports_index_simulation(self):
        return True
    
    def _simulate_index(self, index):
        pass # TODO

    def _drop_simulated_index(self, oid):
        pass # TODO

    def create_index(self, index):
        pass # TODO

    def drop_indexes(self):
        pass # TODO

    def exec_query(self, query, timeout=None, cost_evaluation=False):
        pass # TODO

    def _cleanup_query(self, query):
        pass # TODO

    def _get_cost(self, query):
        pass # TODO

    def _get_plan(self, query):
        pass # TODO

    def number_of_indexes(self):
        pass # TODO

    def table_exists(self, table_name):
        pass # TODO

    def database_exists(self, database_name):
        pass # TODO
