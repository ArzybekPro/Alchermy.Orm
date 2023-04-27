import sqlalchemy as db
from sqlalchemy import select



engine = db.create_engine('postgresql://macbook_air:arzybek0912@localhost:5432/postgres', echo=True)
connection = engine.connect()
metadata = db.MetaData()

products = db.Table('products', metadata,
    db.Column('product_id', db.Integer, primary_key=True),
    db.Column('product_name', db.Text),
    db.Column('supplier_name', db.Text),
    db.Column('price', db.Integer)
)

metadata.create_all(engine)

ins_product_query = products.insert().values([
    {'product_name': 'banana', 'supplier_name': 'united banana', 'price': 200},
    {'product_name': 'alma', 'supplier_name': 'united alma', 'price': 300},
    {'product_name': 'kartoshko', 'supplier_name': 'united kartoshko', 'price': 400},
])

# connection.execute(ins_product_query)

select_all_products = db.select([
    products.c.product_id, 
    products.c.product_name, 
    products.c.supplier_name, 
    products.c.price
])
select_all_result = connection.execute(select_all_products)
print(select_all_result.fetchall())


select_query_price = db.select([products].where(products.columns.price ==300))
select_price_result = connection.execute(select_query_price)
print(select_price_result.fetchall())


# Update
update_query = db.update(products).where(products.columns.supplier_name=='united alma').values(supplier_name =='united fruits')
connection.execute(update_query)

select_all_products = db.select([
    products.c.product_id, 
    products.c.product_name, 
    products.c.supplier_name, 
    products.c.price
])
select_all_result = connection.execute(select_all_products)
print(select_all_result.fetchall())





#Delete
delete_query = db.delete(products).where(products.columns.supplier_name == 'united banana')
connection.execute(delete_query)
select_all_products = db.select([
    products.c.product_id, 
    products.c.product_name, 
    products.c.supplier_name, 
    products.c.price
])
select_all_result = connection.execute(select_all_products)
print(select_all_result.fetchall())
