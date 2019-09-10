# Created by lujin at 31/3/2017
import pymysql

from littledb.database import MySQLDatabase
from littledb.utils import ParseMixin

config = {
    'host': '112.124.99.208',
    'port': 3306,
    'user': 'root',
    'password': 'root',
    'database': 'idoo',
    'charset': 'utf8mb4',
    'cursorclass': pymysql.cursors.DictCursor
}


database = MySQLDatabase(**config)
database.init_pool(2)


class Field(object):
    def __init__(self, column_type, primary_key, default, key, nullable, comment):
        self._column_type = column_type
        self._primary_key = primary_key
        self._default = default
        self._key = key
        self._nullable = nullable
        self._comment = comment


class StringField(Field):
    def __init__(self, ddl='varchar(100)', primary_key=False, default=None, key=False, nullable=False, comment=None):
        super(StringField, self).__init__(ddl, primary_key, default, key, nullable, comment)


class IntegerField(Field):
    def __init__(self, ddl='int(11)', primary_key=False, default=None, key=False, nullable=False, comment=None):
        super(IntegerField, self).__init__(ddl, primary_key, default, key, nullable, comment)


class ModelMetaclass(type):
    def __new__(cls, name, bases, attrs):
        if name == 'Model':
            return type.__new__(cls, name, bases, attrs)
        table_name = attrs.get('__table__', None) or name
        attrs['__table__'] = table_name
        return type.__new__(cls, name, bases, attrs)


class Model(dict, ParseMixin, metaclass=ModelMetaclass):
    def __init__(self, **kwargs):
        super(Model, self).__init__(**kwargs)
        self._sql = None
        self._left_models = []
        self._where = []
        self._args = []
        self._join = []
        self._order_by = ''
        self._limit = ''
        self._set = ''
        self.results = None

    def __getattr__(self, item):
        try:
            return self[item]
        except KeyError:
            raise AttributeError("'Model' object has no attribute '%s'" % item)

    def __setattr__(self, key, value):
        self[key] = value

    def left_join(self, model=None, on=None, concat=' and ', join_sql=None):
        print(model)
        if join_sql is None:
            model = model()
            on = [('%s.%s' % (self.__table__, item[0]), '%s.%s' % (model.__table__, item[1])) for item in on]
            on_sql = self.parse_join(on, concat)
            join_sql = 'left join %s on %s' % (model.__table__, on_sql)
            self._left_models.append(model)
        if join_sql:
            self._join.append(join_sql)

        return self

    def where(self, args=None, concat=' and ', next_concat=None, where_sql=None, where_args=None):
        if where_sql is None:
            where_sql, where_args = self.parse_where(args, concat)

        if where_sql:
            where_sql = '(%s)' % where_sql if next_concat is not None else where_sql
            self._where.append(where_sql)
            if next_concat is not None:
                self._where.append(next_concat)
        if where_args:
            self._args += where_args

        return self

    def order_by(self, args=None, order_by_sql=None):
        if order_by_sql is None and args:
            order_by_sql = self.parse_order_by(args)

        if order_by_sql:
            self._order_by = order_by_sql
        return self

    def limit(self, page=1, size=10):
        limit_sql, limit_args = self.parse_limit(page, size)
        self._limit = limit_sql
        self._args += limit_args
        return self

    def set(self, sets):
        set_sql, set_args = self.parse_set(sets)
        self._set = set_sql
        self._args += set_args
        return self

    @property
    def sql(self):
        return self._sql

    def find_all(self, cols=None, orm=False):
        cols = ', '.join(cols) if cols else '*'
        self._sql = 'SELECT %s FROM %s ' % (cols, self.__table__)
        if self._join:
            self._sql += ' '.join(self._join)
        if self._where:
            self._sql += ' where '+' '.join(self._where)
        if self._order_by:
            self._sql += self._order_by
        if self._limit:
            self._sql += self._limit
        print(self._sql)
        print(self._args)
        rows = database.query(self._sql, self._args)
        if orm:
            pass
        return rows

    def update(self):
        self._sql = ['update %s ' % self.__table__]

        if self._set:
            self._sql.append(self._set)

        if self._join:
            self._sql.append(' '.join(self._join))

        if self._where:
            self._sql.append('where %s' % ' '.join(self._where))
        sql = ' '.join(self._sql)
        # print('update: ', ' '.join(self._sql), self._args)
        affect = database.execute(sql, self._args)
        return affect

