# Created by lujin at 31/3/2017
import pymysql

from littledb.DBUtils.PooledDB import PooledDB


def init_pool(**config):
    pass


def connection(**config):
    return pymysql.connect(**config)


def execute():
    pass
config = {
    'host': '112.124.99.208',
    'port': 3306,
    'user': 'root',
    'password': 'root',
    'database': 'idoo',
    'charset': 'utf8mb4',
    'cursorclass': pymysql.cursors.DictCursor
}


pool = PooledDB(pymysql, 5, **config)
db = pool.connection()
cursor = db.cursor()
sql = 'SELECT * FROM sale_logistics'
cursor.execute(sql)
result = cursor.fetchmany()
print(result)
