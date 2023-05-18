import unittest

from selection.dbms.tidb_dbms import TiDBDatabaseConnector
from selection.index import Index
from selection.workload import Column, Query, Table


class TestDatabaseTiDB(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        pass

    @classmethod
    def tearDownClass(cls):
        pass

    def test_database_names(self):
        db = TiDBDatabaseConnector("test")
        print(db.database_names())

    def test_hypo_index(self):
        db = TiDBDatabaseConnector("test")
        db.exec_only('drop table if exists tx')
        db.exec_only('create table tx (a int, b int, c int)')

        col_a = Column("a")
        col_b = Column("b")
        tx = Table("tx")
        tx.add_column(col_a)
        tx.add_column(col_b)
        index_a = Index([col_a])
        index_ab = Index([col_a, col_b])
        name_a = db._simulate_index(index_a)
        name_ab = db._simulate_index(index_ab)
        print(db.show_simulated_index('tx'))
        db.drop_simulated_index(name_ab)
        print(db.show_simulated_index('tx'))

    def test_cost(self):
        db = TiDBDatabaseConnector("test")
        db.exec_only('drop table if exists tx')
        db.exec_only('create table tx (a int, b int, c int)')

        q = Query(1, "select a from tx where a<10")
        print(db.get_plan(q))
        print(db.get_cost(q))

        col_a = Column("a")
        tx = Table("tx")
        tx.add_column(col_a)
        index_a = Index([col_a])
        db._simulate_index(index_a)
        print(db.show_simulated_index('tx'))

        print(db.get_plan(q)) # TableScan -> IndexScan
        print(db.get_cost(q))

if __name__ == "__main__":
    unittest.main()
