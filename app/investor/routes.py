from app.investor import blueprint
from app import db
from flask import jsonify, json
import pandas as pd


@blueprint.route('/GetList')
def GetList():

    return "Hello World - Portfolio"



