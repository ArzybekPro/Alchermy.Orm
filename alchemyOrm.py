# from sqlalchemy import create_engine,select,Table,Column,Integer,String,MetaData,ForeignKey

# meta = MetaData()

# authors = Table("Authors",meta,
#     Column('id_author',Integer,primary_key=True),
#     Column('name',String(250),nullable=False)
# )

# books = Table('books',meta,
#     Column('id_books',Integer,primary_key=True),
#     Column('title',String(250),nullable=False),
#     Column('authors_id',Integer,ForeignKey('Authors.id_author')),
#     Column('genre',String(200)),
#     Column('price',Integer)       
# )

# # print(books.c.authors_id)
# # print(books.primary_key)

# engine = create_engine('postgresql://macbook_air:arzybek0912@localhost:5432/postgres',echo=True)
# meta.create_all(engine)

# conn = engine.connect()

# ins_author_query=authors.insert().values(name='Arzybek')
# result=conn.execute(ins_author_query)
# authors_id=result.inserted_primary_key[0]


# ins_books_query = books.insert().values(title='Swon',authors_id = authors_id,genre='Education',price=1900)
# conn.execute(ins_books_query)
# ins_books_query2 = books.insert().values(title='Python',authors_id = authors_id,genre='Matiovation',price=2000)
# conn.execute(ins_books_query2)


# books_gr_1000_query = books.select().where(books.c.price > 1000)
# result = conn.execute(books_gr_1000_query)
# for row in result:
#     print(row)

# s = select([books, authors]).where(books.c.id_books == authors.c.id_author)
# result = conn.execute(s)
# for row in result:
#     print(row)



import sqlalchemy as db
engine = db.create_engine('postgresql://macbook_air:arzybek0912@localhost:5432/postgres',echo=True)


connection = engine.connect()


result = connection.execute(db.text("select pg_catalog.version()"))
print(result.fetchall())
result = connection.execute(db.text("select current_schema()"))
print(result.fetchall())
result = connection.execute(db.text("show standard_conforming_strings"))
print(result.fetchall())

transaction = connection.begin()

try:
    
    author = {"name": "Arzybek"}
    result = connection.execute(db.text("INSERT INTO Authors (name) VALUES (:name) RETURNING id_author"), **author)
    author_id = result.fetchone()[0]


    book1 = {"title": "Swon", "authors_id": author_id, "genre": "Education", "price": 1900}
    result = connection.execute(db.text("INSERT INTO books (title, authors_id, genre, price) VALUES (:title, :authors_id, :genre, :price) RETURNING id_books"), **book1)
    book1_id = result.fetchone()[0]

    book2 = {"title": "Python", "authors_id": author_id, "genre": "Matiovation", "price": 2000}
    result = connection.execute(db.text("INSERT INTO books (title, authors_id, genre, price) VALUES (:title, :authors_id, :genre, :price) RETURNING id_books"), **book2)
    book2_id = result.fetchone()[0]

    result = connection.execute(db.text("SELECT id_books, title, authors_id, genre, price FROM books WHERE price > :price"), price=1000)
    print(result.fetchall())


    books = db.Table('books', db.MetaData(), autoload=True, autoload_with=engine)
    authors = db.Table('Authors', db.MetaData(), autoload=True, autoload_with=engine)
    s = db.select([books, authors]).where(books.c.authors_id == authors.c.id_author)
    result = connection.execute(s)
    print(result.fetchall())

 
    transaction.commit()

except:
    transaction.rollback()
    raise

finally:

    connection.close()

