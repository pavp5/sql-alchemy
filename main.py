import sqlalchemy
from sqlalchemy.orm import sessionmaker
import model
from model import Sale, Stock, Book, Shop, Publisher
import json


# Создание таблиц
def create_tables(engine):
    model.Base.metadata.drop_all(engine)
    model.Base.metadata.create_all(engine)


# Загрузка данных из json-файла
def load_json(file_path, session):
    Session = sessionmaker(bind=engine)
    session = Session()

    # Удаление старых данных
    session.query(Sale).delete()
    session.query(Stock).delete()
    session.query(Book).delete()
    session.query(Shop).delete()
    session.query(Publisher).delete()

    # Загрузка данных
    with open(file_path, encoding="utf-8") as f:
        for row in json.load(f):
            if row["model"] == "publisher":
                session.add(Publisher(
                    id=row["pk"],
                    name=row["fields"]["name"]))
            elif row["model"] == "book":
                session.add(Book(
                    id=row["pk"],
                    title=row["fields"]["title"],
                    id_publisher=row["fields"]["id_publisher"]))
            elif row["model"] == "shop":
                session.add(Shop(
                    id=row["pk"],
                    name=row["fields"]["name"]))
            elif row["model"] == "stock":
                session.add(Stock(
                    id=row["pk"],
                    id_shop=row["fields"]["id_shop"],
                    id_book=row["fields"]["id_book"],
                    count=row["fields"]["count"]))
            elif row["model"] == "sale":
                session.add(Sale(
                    id=row["pk"],
                    price=row["fields"]["price"],
                    date_sale=row["fields"]["date_sale"],
                    count=row["fields"]["count"],
                    id_stock=row["fields"]["id_stock"]))

    session.commit()
    session.close()


# Запрос выборки продаж целевого издателя
def get_shops(publisher, engine):
    if len(str(publisher).strip()) == 0:
        return None

    Session = sessionmaker(bind=engine)
    session = Session()

    if publisher.isdigit():
        query = (
            session.query(Book.title, Shop.name, Sale.price, Sale.date_sale)
                .join(Publisher).join(Stock).join(Shop).join(Sale)
                .filter(Publisher.id == publisher)
        )
    else:
        query = (
            session.query(Book.title, Shop.name, Sale.price, Sale.date_sale)
                .join(Publisher).join(Stock).join(Shop).join(Sale)
                .filter(Publisher.name.ilike(f"%{publisher}%")))

    for title, name, price, date_sale in query.all():
        print(f"{title: <40} | {name: <10} | {price: <8} | {date_sale.strftime('%d-%m-%Y')}")

    session.close()


if __name__ == "__main__":
    DSN = "postgresql://postgres:ps3rpos6@localhost:5432/bookshop_db"
    engine = sqlalchemy.create_engine(DSN)

    # Создание таблиц БД
    create_tables(engine)

    # Загрузка данных из файла json
    load_json("tests_data.json", engine)

    # Запрос выборки продаж целевого издателя   
    get_shops(input('Insert Publisher name or id: '), engine)
