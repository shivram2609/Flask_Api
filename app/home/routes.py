from app.home import blueprint
from app import db
from flask import Flask, jsonify, request, json
from datetime import datetime
import pandas as pd
import functools


@blueprint.route('/')
def GetList():

    return "Hello World - Base"


@blueprint.route("/GetLibraryAllFolders/")
def GetLibraryAllFolders():

    orgID = 2  # request.args.get("orgID")
    investmentID = 4  # request.args.get("investmentID")
    conn = db.engine.connect()
    print(orgID, investmentID, conn)

    sql = f"SELECT * FROM v_getlibraryallfolders WHERE id_org= {orgID} AND id_investment={investmentID} AND orgportinvprop='investment'"
    rawdata = pd.read_sql_query(sql, conn)
    data = rawdata[{'id_filefolder', 'folder_name',
                    'folder_layout', 'category_name'}]
    data_json = data.to_json(orient="records")
    data_json = json.loads(data_json)

    return jsonify({"data": data_json})


@blueprint.route("/GetLibraryFileList/")
def GetLibraryFileList():

    orgID = 2 or request.args.get("orgID")
    folderID = request.args.get("folderID")
    conn = db.engine.connect()

    sql = f"SELECT * FROM v_getlibraryfolderfiles WHERE id_org={OrgID} AND id_filefolder={FolderID}"
    rawdata = pd.read_sql_query(sql, conn)
    data = rawdata[{'id_file', 'file_name', 'file_date',
                    'file_date_uploaded', 'file_url', 'folder_name'}]
    data_json = data.to_json(orient="records")
    data_json = json.loads(data_json)

    return jsonify({"data": data_json})


@blueprint.route("/GetLibraryFileDetail/")
def GetLibraryFileDetail():

    conn = db.engine.connect()
    orgID = 2 or request.args.get("orgID")
    fileID = request.args.get("fileID")

    sql = f"select * from v_getlibrayfiledetail where id_file={FileID} and id_org ={OrgID}"
    rawdata = pd.read_sql_query(sql, conn)
    data = rawdata[{'id_file', 'field_name', 'field_datatype', 'meta_value'}]
    data_json = data.to_json(orient="records")
    data_json = json.loads(data_json)

    return jsonify({"data": data_json})


@blueprint.route("/GetAllPortfolioCards/")
def GetAllPortfolioCards():

    conn = db.engine.connect()
    orgID = 2 or request.args.get("orgID")

    sql = f"select * from tbl_dummy_getallportfolios"
    rawdata = pd.read_sql_query(sql, conn)
    data = rawdata
    data_json = data.to_json(orient="records")
    data_json = json.loads(data_json)

    return jsonify({"data": data_json})

@blueprint.route("/GetAllPortfolioList/")
def GetAllPortfolioList():

    conn = db.engine.connect()
    orgID = 2 or request.args.get("orgID")

    sql = f"select id_org, id_portfolio, portfolio_name from tbl_portfolios where id_org = {orgID} order by portfolio_name "
    rawdata = pd.read_sql_query(sql, conn)
    data = rawdata
    data_json = data.to_json(orient="records")
    data_json = json.loads(data_json)

    return jsonify({"data": data_json})


@blueprint.route("/GetAllTransactions/")
def GetAllTransactions():

    conn = db.engine.connect()
    orgID = 2 or request.args.get("orgID")

    sql = f"select * from tbl_dummy_gettransactions"
    rawdata = pd.read_sql_query(sql, conn)
    data = rawdata
    data_json = data.to_json(orient="records")
    data_json = json.loads(data_json)

    return jsonify({"data": data_json})