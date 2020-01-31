from flask import Flask
from flask_cors import CORS
from flask_sqlalchemy import SQLAlchemy
from importlib import import_module
from os import path
import yaml


db = SQLAlchemy()


def register_extensions(app):
    db.init_app(app)


def register_blueprints(app):
    for module_name in ('home', 'investment', 'portfolio','investor'):
        module = import_module('app.{}.routes'.format(module_name))
        app.register_blueprint(module.blueprint)


def create_app():
    app = Flask(__name__)
    cors = CORS(app)
    f = open("config.yaml", "r")
    config = yaml.load(f, Loader=yaml.BaseLoader)

    app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False
    app.config["SQLALCHEMY_POOL_RECYCLE"] = 3600
    app.config["SQLALCHEMY_ECHO"] = False
    app.config["WTF_CSRF_ENABLED"] = config["SQLALCHEMY"]["WTF_CSRF_ENABLED"]
    app.config["SECRET_KEY"] = config["SQLALCHEMY"]["SECRET_KEY"]
    app.config["SQLALCHEMY_DATABASE_URI"] = "mysql+pymysql://{}:{}@{}:{}/{}".format(
        config["DB"]["user"],
        config["DB"]["password"],
        config["DB"]["host"],
        config["DB"]["port"],
        config["DB"]["database"],
    )

    register_extensions(app)
    register_blueprints(app)

    return app
