from app import db
from flask import Flask, jsonify, request, json
from datetime import datetime
from dateutil.relativedelta import relativedelta
import pandas as pd
from .functions import *

from app.investment import blueprint


@blueprint.route('/GetList', methods=["GET"])
def GetList():

    orgID = 2 or request.args.get("orgID")
    portfolioID = request.args.get("portfolioID")

    conn = db.engine.connect()

    sql = f"select investmentID, Capital_name, Property_address1, 'Active' as status, Property_type, debt_type, property_buildings, debt_date_funding, debt_date_maturity, capital_amount, capital_balance, debt_index_type, debt_spread, property_latitude, property_longitude from v_getInvestment where orgID={orgID} and portfolioID={portfolioID}"
    data = pd.read_sql_query(sql, conn)

    data_json = data.to_json(orient="records")
    data_json = json.loads(data_json)

    dataKPI = [
        ['Total', 0],
        ['Active', 0],
        ['Retired', 0],
        ['Pipeline', 0],
    ]
    colKPI = ['Type', 'Number']
    dataKPI = pd.DataFrame(dataKPI, columns=colKPI)

    dataKPI.loc[dataKPI['Type'] == 'Total', 'Number'] = data.count()[
        'investmentID']
    dataKPI.loc[dataKPI['Type'] == 'Active',
                'Number'] = data[data['status'] == 'Active'].count()['investmentID']
    dataKPI.loc[dataKPI['Type'] == 'Retired',
                'Number'] = data[data['status'] == 'Retired'].count()['investmentID']
    dataKPI.loc[dataKPI['Type'] == 'Pipeline',
                'Number'] = data[data['status'] == 'Pipeline'].count()['investmentID']

    dataKPI_json = dataKPI.to_json(orient="records")
    dataKPI_json = json.loads(dataKPI_json)

    return jsonify({"KPI": dataKPI_json, "data": data_json})

@blueprint.route("/GetPropertyList")
def GetPropertyList():

    orgID = request.args.get("orgID")
    investmentID = request.args.get("investmentID")

    conn = db.engine.connect()
    sql = f"select * from v_getinvestmentpropertylist where id_org = {orgID} and id_investment={investmentID}"
    data = pd.read_sql_query(sql, conn)

    data_json = data.to_json(orient="records")
    data_json = json.loads(data_json)

    return jsonify({"data": data_json})


@blueprint.route("/GetLoanSummary")
def GetLoanSummary():

    orgID = request.args.get("orgID")
    investmentID = request.args.get("investmentID")

    conn = db.engine.connect()
    sql = f"select * from v_getLoanSummary where id_investment={investmentID}"
    data = pd.read_sql_query(sql, conn)

    data_json = data.to_json(orient="records")
    data_json = json.loads(data_json)

    return jsonify({"data": data_json})


@blueprint.route("/GetAssetSummary")
def GetAssetSummary():

    orgID = request.args.get("orgID")
    investmentID = request.args.get("investmentID")

    conn = db.engine.connect()
    sql = f"select * from v_getAssetSummary where id_investment={investmentID}"
    data = pd.read_sql_query(sql, conn)

    data_json = data.to_json(orient="records")
    data_json = json.loads(data_json)

    return jsonify({"data": data_json})


@blueprint.route("/GetComment", methods=["GET"])
def GetInvestmentComments():

    comment_type = request.args.get("comment_type")
    investmentID = request.args.get("investmentID")

    conn = db.engine.connect()
    sql = f"select comment from tbl_investments_comment where id_investment=1 and comment_type={comment_type}"

    data = pd.read_sql_query(sql, conn)

    data_json = data.to_json(orient="records")
    data_json = json.loads(data_json)
    return jsonify({"data": data_json})


@blueprint.route("/GetReserve")
def GetReserve():

    investmentID = request.args.get('investmentID')
    conn = db.engine.connect()
    sql = f"select * from v_getinvestmentreserve where id_investment = {investmentID}"
    rawdata = pd.read_sql_query(sql, conn)
    rawdata = rawdata.sort_values(by='funding_date', ascending=False)

    reserveData = [
        ['Interest Reserve', 'Servicer Balance - Interest', 0.0, 0.0, 0.0, 0.0, 0.0],
        ['Renovation Reserve', 'Servicer Balance - Renovation', 0.0, 0.0, 0.0, 0.0, 0.0],
        ['TILC Reserve', 'Servicer Balance - TILC', 0.0, 0.0, 0.0, 0.0, 0.0],
        ['Other Reserve', 'Servicer Balance - Other', 0.0, 0.0, 0.0, 0.0, 0.0],
    ]
    reserveCol = [
        'Reserve Type',
        'Servicer Type',
        'Total Reserve',
        'Amount Funded to Borrower',
        '% Funded',
        'Balance at Servicer',
        'Amount Funded to Servicer'
    ]
    reserveTable = pd.DataFrame(reserveData, columns=reserveCol)

    loanTable = {'Gross Loan Amount': 0.0,
                 'Cumulative Principal Payment': 0.0, 'Cumulative Interest Payment': 0.0}

    for index, row in reserveTable.iterrows():
        try:
            reserveTable['Total Reserve'].values[index] = rawdata[rawdata['folder_name'] == row['Reserve Type']].max()[
                'folder_amount']
            reserveTable['Amount Funded to Servicer'].values[index] = rawdata[rawdata['folder_name'] == row['Reserve Type']].sum()[
                'funding_amount']
            reserveTable['Balance at Servicer'].values[index] = rawdata[rawdata['funding_activity']
                                                                        == row['Servicer Type']].iloc[0].funding_amount
            reserveTable['Amount Funded to Borrower'].values[index] = reserveTable['Amount Funded to Servicer'].values[index] - \
                reserveTable['Balance at Servicer'].values[index]
            reserveTable['% Funded'].values[index] = reserveTable['Amount Funded to Borrower'].values[index] / \
                reserveTable['Total Reserve'].values[index]
        except:
            pass

    reserveChart = reserveTable[{'Reserve Type', '% Funded'}]
    reserveChart['% Unfunded'] = 1-reserveChart['% Funded']
    reserveChart_json = reserveChart.to_json(orient='records')
    reserveChart_json = json.loads(reserveChart_json)

    loanTable['Gross Loan Amount'] = rawdata[rawdata['funding_activity']
                                             == 'Principal Funding'].max()['folder_amount']
    loanTable['Cumulative Principal Payment'] = rawdata[rawdata['funding_activity']
                                                        == 'Principal Funding'].sum()['funding_amount']*-1
    loanTable['Cumulative Interest Payment'] = rawdata[rawdata['funding_activity']
                                                       == 'Interest Payment'].sum()['funding_amount']
    loanTable_json = json.dumps(loanTable)

    reserveTable_json = reserveTable.to_json(
        orient="records", date_format="iso")
    reserveTable_json = json.loads(reserveTable_json)

    dataTable = rawdata[{'funding_date', 'folder_name',
                         'funding_activity', 'funding_amount', 'funding_description'}]
    dataTable_json = dataTable.to_json(orient="records", date_format="iso")
    dataTable_json = json.loads(dataTable_json)

    return jsonify({"Reserve Chart": reserveChart_json, "Loan Table": loanTable_json, "Reserve Table": reserveTable_json, "Data Table": dataTable_json})


@blueprint.route("/GetCF", methods=['GET'])
def GetCF():

    orgID = request.args.get('orgID') or 2
    investmentID = request.args.get('investmentID') or 668
    propertyID = request.args.get('propertyID') or 829
    financialType = "Actual" or request.args.get('financialType')
    year = request.args.get('year')

    conn = db.engine.connect()
    sql = f"select * from v_getCF where OrgId={orgID} and id_property={propertyID} and financials_type ='{financialType}' and iscurrent=1 and value <> 0 and left(period,4) = {year}"
    rawdata = pd.read_sql_query(sql, conn)
    sql = f"select * from tbl_account_ref where id_org ={orgID}"
    acctRef = pd.read_sql_query(sql, conn)
    acctRef['prefix'] = acctRef['account_gl'].apply(str).str[:2]

    try:

        data = sumCF(rawdata, acctRef)
        result = pivotCF(data, acctRef, "period")

        result_json = result.to_json(orient="records", date_format="iso")
        result_json = json.loads(result_json)

        header = list(result)
        header_json = json.dumps(header)
        header_json = json.loads(header_json)

        return jsonify({"header": header_json, "data": result_json})

    except IndexError:
        return jsonify({"header": [], "data": []})


@blueprint.route("/GetVariance", methods=['GET'])
def GetVariance():

    conn = db.engine.connect()

    orgID = int(request.args.get('orgID')) or 2
    investmentID = int(request.args.get('investmentID')) or 668
    propertyID = int(request.args.get('propertyID')) or 829
    financialType = request.args.get('financialType') or "Actual"
    year = int(request.args.get('year'))
    interval = str(request.args.get('interval')).lower() or 'quarter'

    intervalGrp = {'annual': [year], 'quarter': [1, 2, 3, 4]}

    sql = f"select * from v_getCF where OrgId={orgID} and id_property={propertyID} and financials_type = ('{financialType}' or 'Underwriting') and iscurrent=1 and value <> 0 and left(period,4) = {year}"
    rawdata = pd.read_sql_query(sql, conn)
    sql = f"select * from tbl_account_ref where id_org ={orgID}"
    acctRef = pd.read_sql_query(sql, conn)
    acctRef['prefix'] = acctRef['account_gl'].apply(str).str[:2]

    try:
        data = sumCF(rawdata, acctRef)
        pivotCol = ['financials_type', interval]
        pivotData = pivotCF(data, acctRef, pivotCol)
        pivotData.columns = pivotData.columns.ravel()
        pivotData = pivotData.reset_index()
        pivotData = pivotData.fillna(0)
        colHeader = ['account_caption', 'html_class', ('account_gl', '')]
        for item in intervalGrp[interval]:
            colHeader.append(('Underwriting', item))
            colHeader.append((financialType, item))
            colHeader.append(('diff', item))

        pivotData = pivotData.reindex(columns=colHeader, fill_value=0)
        pivotData_json = []
        pivotHeader_json = ['account_capition', 'html_class', 'Underwriting', financialType, 'diff']
        for item in intervalGrp[interval]:
            temp = pivotData[{'account_caption', 'html_class'}]
            temp['account_gl'] = pivotData[('account_gl', '')]
            temp['Underwriting'] = pivotData[('Underwriting', item)]
            temp[financialType] = pivotData[(financialType, item)]
            temp['diff'] = temp['Underwriting'] - temp[financialType]
            temp_json = temp.to_json(orient='records')
            temp_json = json.loads(temp_json)
            pivotData_json.append({str(item): temp_json})

        return jsonify({'data': pivotData_json, 'header': pivotHeader_json})

    except:
        return jsonify({'data': [], 'header': []})


@blueprint.route("/GetRentRoll")
def GetInvestmentRentRoll():

    orgID = request.args.get('orgID') or 2
    propertyID = request.args.get('propertyID') or 829

    conn = db.engine.connect()
    sql = f"select * from v_getrentrolls where id_property={propertyID} and id_org={orgID}"
    rawdata = pd.read_sql_query(sql, conn)
    data = rawdata[{'id_property', 'period', 'occupancy'}]

    data_json = data.to_json(orient="records", date_format="iso")
    data_json = json.loads(data_json)
    return jsonify({"data": data_json})


@blueprint.route("/GetRentRollDetail")
def GetInvestmentRentRollDetail():

    propertyID = request.args.get('propertyID') or 829
    year = request.args.get('year') or 2019

    conn = db.engine.connect()
    sql = f"select * from v_getrentrolltenants where  id_property={propertyID} and YEAR(period)={year} "
    rawdata = pd.read_sql_query(sql, conn)
    data = rawdata.copy()

    try:
        data['period'] = data['period'].dt.strftime("%Y-%m")
        datapivot_tenant = pd.pivot_table(
            data,
            values="tenant_monthlyrent_collected",
            index=['tenant_name'],
            columns='period'
        )
        datapivot_suite = pd.pivot_table(
            data,
            values="tenant_monthlyrent_collected",
            index=['tenant_suite_number'],
            columns='period'
        )

        datapivot_tenant = datapivot_tenant.reset_index()
        datapivot_tenant.fillna(0)
        data_tenant_json = datapivot_tenant.to_json(orient="records")
        data_tenant_json = json.loads(data_tenant_json)

        datapivot_suite = datapivot_suite.reset_index()
        datapivot_suite.fillna(0)
        data_suite_json = datapivot_suite.to_json(orient="records")
        data_suite_json = json.loads(data_suite_json)

        return jsonify({"tenant": data_tenant_json, "suite": data_suite_json})

    except:
        return jsonify({"tenant": [], "suite": []})

# 3. dashboard
@blueprint.route("/GetFinancialWidget")
def GetFinancialWidget():

    orgID = request.args.get("orgID") or 2
    propertyID = request.args.get("propertyID") or 829
    reportDate = request.args.get("reportDate") or '2019-12-31'
    account = request.args.get("account")
    intervalType = "Month" or request.args.get("intervalType")

    conn = db.engine.connect()

    try:
        reportDate = datetime.strptime(reportDate, '%Y-%m-%d')
        reportDate_start = reportDate - relativedelta(months=+12)
        sql = f"select * from v_getCF where OrgId=1 and id_property={propertyID} and financials_type in ( 'Original', 'Budget') and iscurrent=1 and value <> 0 and period_date between '{reportDate_start}' and '{reportDate}'"
        rawdata = pd.read_sql_query(sql, conn)
        sql = f"select * from tbl_account_ref where id_org ={orgID}"
        acctRef = pd.read_sql_query(sql, conn)
        acctRef['prefix'] = acctRef['account_gl'].apply(str).str[:2]

        data = sumCF(rawdata, acctRef)
        data = data.loc[data['account_caption'] == account]
        dataPivot = pd.pivot_table(
            data,
            values="value",
            index=["period"],
            columns="financials_type",
        )
        dataPivot = dataPivot.reset_index()
        data_json = dataPivot.to_json(orient="records")
        data_json = json.loads(data_json)

        return jsonify({"data": data_json})

    except:
        return jsonify({"data": []})


@blueprint.route("/GetKPIWidget")
def GetKPIWidget():

    orgID = request.args.get("orgID") or 2
    propertyID = request.args.get("propertyID") or 829
    reportDate = request.args.get("reportDate") or "2019-12-31"
    account = request.args.get("account")

    conn = db.engine.connect()

    try:
        reportDate = datetime.strptime(reportDate, '%Y-%m-%d')
        reportDate_start = reportDate - relativedelta(months=+12)
        sql = f"select * from v_getpropertykpi where  id_property={propertyID} and period between '{reportDate_start}' and '{reportDate}'"
        rawdata = pd.read_sql_query(sql, conn)
        data = rawdata[{account, 'period'}]
        data['period'] = data['period'].dt.strftime("%Y-%m")

        data_json = data.to_json(orient="records", date_format='iso')
        data_json = json.loads(data_json)

        return jsonify({"data": data_json})

    except:
        return jsonify({"data": []})


@blueprint.route("/GetYearList")
def GetYearList():

    conn = db.engine.connect()

    orgID = request.args.get("orgID") or 2
    investmentID = request.args.get("investmentID") or 829

    sql = f"call realera.sp_GetYearList({orgID}, {investmentID})"
    rawdata = pd.read_sql_query(sql, conn)
    data=rawdata['year'].sort_values()
    data_json = data.to_json(orient="records", date_format='iso')
    data_json = json.loads(data_json)

    return jsonify({"data": data_json})
