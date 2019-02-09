from flask_sqlalchemy import SQLAlchemy


def init_db(app):
    global db
    db = SQLAlchemy(app)
    db.create_all()


