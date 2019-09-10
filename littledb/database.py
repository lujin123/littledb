# Created by lujin at 31/3/2017
import functools
import traceback
from contextlib import contextmanager

import pymysql

from littledb.DBUtils.PooledDB import PooledDB
# from littledb.DBUtils.SimplePooledDB import PooledDB


class Database(object):
    pass


class MySQLDatabase(Database):
    """
    数据库连接池对象，这个对象需要在项目中保持唯一，否则对于事务的操作会有问题，会出什么错误不清楚了

    数据库只能同时开启一个事务，多个事务开启后会导致前面的事务自动提交，
    还有一些其他操作会导致事务的自动提交，需要避免这样的操作

    目前 self._connection 这个属性使用来保存开启了事务的那个连接的，所以在这个连接存在的时候，
    后面对数据库的操作都会使用这个连接而且开启了事务，这个行为在同步的代码中应该是没什么问题的，
    因为所有操作都是顺序执行，在一个方法中开启事务后，这个方法中调用的函数也会在事务中执行，正是我们需要的。

    但是如果在异步的代码中，这样做就会有问题，因为开启事务后导致所有的后续获取到的连接都是同一个，
    但是又不一定都是在同一个开启事务的方法中调用的
    """
    def __init__(self, **config):
        self._config = config
        self._pool = None
        self._connection = None
        self._transaction = False

    def init_pool(self, min_cached=0, max_cached=0, max_shared=0, max_connections=0, blocking=False,
                  max_usage=None, set_session=None, reset=True, failures=None, ping=1):
        self._pool = PooledDB(pymysql, min_cached, max_cached, max_shared, max_connections, blocking,
                              max_usage, set_session, reset, failures, ping, **self._config)

    @property
    def pool(self):
        return self._pool

    def get_connection(self):
        return self._connection if self._transaction else self._pool.dedicated_connection()

    def query(self, sql, args=(), size=None):
        with self.connection_ctx() as cursor:
            cursor.execute(sql, args)
            results = cursor.fetchmany(size) if size else cursor.fetchall()
        return results

    def execute(self, sql, args=()):
        sql = self._format_sql(sql)
        with self.connection_ctx() as cursor:
            if self._transaction:
                cursor.connection.autocommit(False)
            affect = cursor.execute(sql, args)
            return affect

    def begin_transaction(self):
        self._connection = self.get_connection()
        self._transaction = True

    def commit(self):
        if self._connection:
            self._connection.commit()
            self.reset_connection()

    def rollback(self):
        if self._connection:
            self._connection.rollback()
            self.reset_connection()

    def reset_connection(self):
        if self._connection:
            self._transaction = False
            self._connection.close()
            self._connection = None

    @contextmanager
    def connection_ctx(self):
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            yield cursor
            if cursor:
                cursor.close()
            if not self._transaction:
                conn.close()
        except Exception as e:
            print(traceback.format_exc())
            print(e)
            self.rollback()

    @contextmanager
    def transaction_ctx(self):
        try:
            self.begin_transaction()
            yield
            self.commit()
        except Exception as e:
            print(e)
            print(traceback.format_exc())
            self.rollback()

    def _format_sql(self, sql):
        return sql.replace('?', '%s')


def transaction(db):
    def _outer(func):
        @functools.wraps(func)
        def _wrapper(*args, **kwargs):
            with db.transaction_ctx():
                return func(*args, **kwargs)
        return _wrapper
    return _outer


def test():
    _config = {
        'host': '112.124.99.208',
        'port': 3306,
        'user': 'root',
        'password': 'root',
        'database': 'idoo',
        'charset': 'utf8mb4',
        'cursorclass': pymysql.cursors.DictCursor
    }
    database = MySQLDatabase(**_config)
    database.init_pool(2)
    # print(database.query('select * from sale_logistics'))
    database.execute('update sale_logistics set delivery_way=%s where id=%s', ('123', 44))
    database.execute('update sale_logistics set delivery_way=%s where id=%s', ('abc', 45))
    database.execute('update sale_logistics set delivery_way=%s where id=%s', ('bcd', 45), True)


if __name__ == '__main__':
    test()
