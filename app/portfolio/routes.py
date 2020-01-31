from app.portfolio import blueprint
from app import db
from flask import jsonify, json, request,Flask
from datetime import date
import pandas as pd


@blueprint.route('/GetList')
def GetList():

    return "Hello World - Portfolio"

@blueprint.route('/GetPortfolioAllocationPropertyType')
def GetPortfolioAllocationPropertyType():

    conn = db.engine.connect()
    OrgID = request.args.get("OrgID")
    PortfolioID = request.args.get("PortfolioID")
    ReportDate =  request.args.get("ReportDate")
    
    sql = f"SELECT property_type, sum(capital_amount) FROM realera.v_getportinvcapproplist WHERE (id_org={OrgID} AND id_portfolio={PortfolioID} AND debt_date_payoff < '1900-12-31' AND debt_date_funding <= '{ReportDate}') or (id_org={OrgID} AND id_portfolio={PortfolioID} AND debt_date_payoff > '{ReportDate}' AND debt_date_funding <= '{ReportDate}') GROUP BY property_type ORDER BY property_type ASC;"
     
    rawdata = pd.read_sql_query(sql, conn)
    portfolio_properties = rawdata[{'sum(capital_amount)', 'property_type'}]
    portfolio_json = portfolio_properties.to_json(orient="records")
    portfolio_json = json.loads(portfolio_json)
    portfolioByPropertyType={}
    for portfolio in portfolio_json:
        if not portfolio['property_type'] in portfolioByPropertyType.keys():
            portfolioByPropertyType[portfolio['property_type']]={"property_type" : portfolio['property_type']}
        portfolioByPropertyType["Portfolio Allocation"]='Gross Loan Amount by Property Type'
        portfolioByPropertyType[portfolio['property_type']]=portfolio['sum(capital_amount)']
    portfolio_data = json.dumps(portfolioByPropertyType)
    portfolio_data = json.loads(portfolio_data)
    return jsonify({"data": portfolio_data, "object": "list"})

@blueprint.route('/GetPortfolioAllocationLocation')
def GetPortfolioAllocationLocation():

    conn = db.engine.connect()
    OrgID = request.args.get("OrgID")
    PortfolioID = request.args.get("PortfolioID")
    ReportDate =  request.args.get("ReportDate")
    
    sql = f"SELECT property_state, sum(capital_amount), allocation_group FROM realera.v_getportfolioallocation_regions WHERE (id_org={OrgID} AND id_portfolio={PortfolioID} AND debt_date_payoff < '1900-12-31' AND debt_date_funding <= '{ReportDate}' AND allocation_category='regions_ncreif') or (id_org={OrgID} AND id_portfolio={PortfolioID} AND debt_date_payoff > '{ReportDate}' AND debt_date_funding <= '{ReportDate}' AND allocation_category='regions_ncreif') GROUP BY allocation_group ORDER BY property_state ASC;"
    
    rawdata = pd.read_sql_query(sql, conn)
    data = rawdata[{'sum(capital_amount)', 'allocation_group'}]
    data_json = data.to_json(orient="records")
    data_json = json.loads(data_json)
    dataByProperty={}
    for portfolio in data_json:
        if not portfolio['allocation_group'] in dataByProperty.keys():
            dataByProperty[portfolio['allocation_group']]={"allocation_group" : portfolio['allocation_group']}
        dataByProperty["Portfolio Allocation"]='Gross Loan Amount by Region'
        dataByProperty[portfolio['allocation_group']]=portfolio['sum(capital_amount)']
    portfolio_data = json.dumps(dataByProperty)
    portfolio_data = json.loads(portfolio_data)
    return jsonify({"data": portfolio_data, "object": "list"})
    
@blueprint.route('/GetPortfolioAllocationLoanSize')
def GetPortfolioAllocationLoanSize():

    conn = db.engine.connect()
    OrgID = request.args.get("OrgID")
    PortfolioID = request.args.get("PortfolioID")
    ReportDate =  request.args.get("ReportDate")
    
    sql = f"select (SELECT sum(capital_amount) FROM realera.v_getportinvcapproplist where capital_amount < 10000000 AND (id_org={OrgID} AND id_portfolio={PortfolioID} AND debt_date_payoff < '1900-12-31' AND debt_date_funding <= '{ReportDate}') or (id_org={OrgID} AND id_portfolio={PortfolioID} AND debt_date_payoff > '{ReportDate}' AND debt_date_funding <= '{ReportDate}') ORDER BY property_type ASC) as amount1 , (SELECT sum(capital_amount) FROM realera.v_getportinvcapproplist where capital_amount >= 10000000 AND capital_amount < 25000000 AND (id_org={OrgID} AND id_portfolio={PortfolioID} AND debt_date_payoff < '1900-12-31' AND debt_date_funding <= '{ReportDate}') or (id_org={OrgID} AND id_portfolio={PortfolioID} AND debt_date_payoff > '{ReportDate}' AND debt_date_funding <= '{ReportDate}') ORDER BY property_type ASC) as amount2, (SELECT sum(capital_amount) FROM realera.v_getportinvcapproplist where capital_amount >= 25000000 AND capital_amount < 50000000 AND (id_org={OrgID} AND id_portfolio={PortfolioID} AND debt_date_payoff < '1900-12-31' AND debt_date_funding <= '{ReportDate}') or (id_org={OrgID} AND id_portfolio={PortfolioID} AND debt_date_payoff > '{ReportDate}' AND debt_date_funding <= '{ReportDate}') ORDER BY property_type ASC) as amount3, (SELECT sum(capital_amount) FROM realera.v_getportinvcapproplist where capital_amount >= 50000000 AND (id_org={OrgID} AND id_portfolio={PortfolioID} AND debt_date_payoff < '1900-12-31' AND debt_date_funding <= '{ReportDate}') or (id_org={OrgID} AND id_portfolio={PortfolioID} AND debt_date_payoff > '{ReportDate}' AND debt_date_funding <= '{ReportDate}') ORDER BY property_type ASC) as amount4;"
    rawdata = pd.read_sql_query(sql, conn)
    gross_loan = rawdata[{'amount1','amount2','amount3','amount4'}]
    data_json = gross_loan.to_json(orient="records")
    data_json = json.loads(data_json)
    groupedByAmount={}
    for amount in data_json:
        # ~ if not portfolio['allocation_group'] in dataByProperty.keys():
            # ~ groupedByAmount[portfolio['allocation_group']]={"allocation_group" : portfolio['allocation_group']}
        groupedByAmount["Portfolio Allocation"]='Gross Loan Amount by Loan Size'
        groupedByAmount['Up to $10MM']=round(amount['amount1'],2)
        groupedByAmount['$10MM to $25MM']=round(amount['amount2'],2)
        groupedByAmount['$25MM to $50MM']=round(amount['amount3'],2)
        groupedByAmount['Greater than $50MM']=round(amount['amount4'],2)
    groupedAmount = json.dumps(groupedByAmount)
    groupedAmount = json.loads(groupedAmount)
    return jsonify({"data": groupedAmount, "object": "list"})
    
@blueprint.route('/GetPortfolioAllocationMaturityPeriod')
def GetPortfolioAllocationMaturityPeriod():
    conn = db.engine.connect()
    today = date.today()
    OrgID = 2 or request.args.get("OrgID")
    PortfolioID = 16 or request.args.get("PortfolioID")
    ReportDate =  today or request.args.get("ReportDate")
    
    sql = f"SELECT id_portfolio, property_name, capital_amount, debt_date_maturity FROM realera.v_getportinvcapproplist WHERE (id_org={OrgID} AND id_portfolio={PortfolioID} AND debt_date_payoff < '1900-12-31' AND debt_date_funding <= '{ReportDate}' AND debt_date_maturity >= '{ReportDate}') or (id_org={OrgID} AND id_portfolio={PortfolioID} AND debt_date_payoff > '{ReportDate}' AND debt_date_funding <= '{ReportDate}' AND debt_date_maturity >= '{ReportDate}') order by property_type ASC;"
    
    rawdata = pd.read_sql_query(sql, conn)
    print(rawdata)
    return
