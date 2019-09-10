# Created by lujin at 15/4/2017
from littledb.error import SQLException


class ParseMixin(object):
    def parse_where(self, args, concat=' and '):
        where = []
        params = []
        for arg in args:
            op = arg[2] if len(arg) == 3 else '='
            if op not in ('in', 'like', '=', '<=', '>=', '<', '>', '!=', 'is'):
                raise SQLException('where condition can not concat with %s' % op)

            key = arg[0]
            value = arg[1]

            if op == 'in':
                if not isinstance(value, (list, tuple)):
                    value = [value]
                where.append('%s %s (%s)' % (key, op, ', '.join(['?' for _ in range(len(value))])))
                params += value
            else:
                where.append('%s %s ?' % (key, op))
                params.append('%'+value+'%' if op == 'like' else value)
        return concat.join(where), params

    def parse_join(self, args, concat=' and '):
        where = []
        for arg in args:
            op = arg[2] if len(arg) == 3 else '='
            where.append('%s %s %s' % (arg[0], op, arg[1]))
        return concat.join(where)

    def parse_order_by(self, args):
        return ' order by ' + ', '.join([' '.join(item) for item in args]) if args else ''

    def parse_limit(self, page, size):
        limit_sql = ' limit %s, %s'
        return limit_sql, [(page-1)*size, size]

    def parse_set(self, sets):
        set_sql = []
        set_args = []
        for k, v in sets.items():
            set_sql.append('%s=%s' % (k, v))
            set_args.append(v)
        set_sql = ' set %s ' % ','.join(set_sql) if set_sql else ''
        return set_sql, set_args
