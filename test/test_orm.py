# Created by lujin at 15/4/2017
from littledb.database import transaction
from littledb.orm import Model, IntegerField, StringField


class SaleLogisticModel(Model):

    __table__ = 'sale_logistics'
    id = IntegerField(ddl='int(11)', primary_key=True)
    tracking_no = StringField(ddl='varchar(32)', comment='tracking no')
    delivery_way = StringField(ddl='varchar(20)')


class SaleOrderModel(Model):
    __table__ = 'sale_order'
    id = IntegerField(ddl='int(11)', primary_key=True, comment='auto increment primary key')

# SaleLogisticModel()\
#     .left_join(SaleOrderModel, [('order_id', 'id')])\
#     .where([('name', 'lujin'), ('id', 12), ('sku_id', [1, 2, 3, 4], 'in')], next_concat='or')\
#     .where([('item_no', '123')])\
#     .order_by([('id', 'desc'), ('name', )])\
#     .limit(1, 20)\
#     .find_all(['id', 'name'])

# id = SaleOrderModel.id


def test2():
    affect2 = SaleLogisticModel().set({'delivery_way': 'xxx'}).where([('id', '1')]).update()
    print('affect2: ', affect2)
    raise Exception('error...')


@transaction(database)
def test():
    rows = SaleLogisticModel().limit(size=1).find_all()
    print('rows: ', rows)
    affect1 = SaleLogisticModel().set({'delivery_way': 'abc'}).where([('id', '1')]).update()
    print('affect1: ', affect1)
    test2()

if __name__ == '__main__':
    test()