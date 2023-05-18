import logging
import re
import pymysql
import time

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
                     port=4000,
                     user='root',
                     password='',
                     database="{}".format(self.db_name),
                     local_infile=True)
        self._cursor = self._connection.cursor()
    
    def enable_simulation(self):
        pass # Do nothing

    def database_names(self):
        result = self.exec_fetch("show databases", False)
        return [x[0] for x in result]
    
    def update_query_text(self, text):
        return text # Do nothing

    def _add_alias_subquery(self, query_text):
        return query_text # Do nothing

    def create_database(self, database_name):
        self.exec_only("create database {}".format(database_name))
        logging.info("Database {} created".format(database_name))

    def import_data(self, table, path, delimiter="|"):
        load_sql = f"load data local infile '{path}' into table {table} fields terminated by '{delimiter}'"
        logging.info(f"load data: {load_sql}")
        self.exec_only(load_sql)

    def indexes_size(self):
        return 0 # TODO

    def drop_database(self, database_name):
        statement = f"DROP DATABASE {database_name};"
        self.exec_only(statement)

        logging.info(f"Database {database_name} dropped")

    def create_statistics(self):
        logging.info("TiDB: Run `analyze`")
        for table_name in self.exec_fetch("show tables", False):
            analyze_sql = "analyze table " + table_name[0]
            logging.info(f"run {analyze_sql}")
            self.exec_only(analyze_sql)

    def set_random_seed(self, value=0.17):
        pass # Do nothing

    def supports_index_simulation(self):
        return True
    
    def show_simulated_index(self, table_name):
        sql = f"show create table {table_name}"
        result = self.exec_fetch(sql)
        hypo_indexes = []
        for line in result[1].split("\n"):
            if "HYPO INDEX" in line:
                tmp = line.split("`")
                hypo_indexes.append(tmp[1])
        return hypo_indexes

    def _simulate_index(self, index):
        table_name = index.table()
        idx_name = f"hypo_{index.index_idx()}"
        statement = (
            f"create index {idx_name} type hypo "
            f"on {table_name} ({index.joined_column_names()})"
        )
        result = self.exec_fetch(statement)
        return f"{table_name}.{idx_name}"

    def _drop_simulated_index(self, ident):
        table_name = ident.split(".")[0]
        idx_name = ident.split(".")[1]
        self.exec_only(f"drop index {idx_name} on {table_name}")

    def create_index(self, index):
        raise Exception("use what-if API")

    def drop_indexes(self):
        raise Exception("use what-if API")

    # run this query and return the actual execution time
    def exec_query(self, query, timeout=None, cost_evaluation=False):
        raise Exception("use what-if API")

    def _cleanup_query(self, query):
        for query_statement in query.text.split(";"):
            if "drop view" in query_statement:
                self.exec_only(query_statement)

    def _get_cost(self, query):
        query_plan = self._get_plan(query)
        cost = query_plan[0][2]
        return cost

    def _get_plan(self, query):
        query_text = self._prepare_query(query)
        statement = f"explain format='verbose' {query_text}"
        query_plan = self.exec_fetch(statement, False)
        self._cleanup_query(query)
        return query_plan
