import pandas as pd


def sumCF(rawdata, acctRef):

    rawdata.iloc[0, 0]
    headerData = [['','',10000, 0], ['','',30000, 0]]
    headerCol = ['period','financials_type', 'account_gl', 'value']
    header = pd.DataFrame(headerData, columns=headerCol)
    rawdata = rawdata.append(header, ignore_index=True)
    dataL0 = rawdata[headerCol]

    col_names = [
        ['L1_acct', 'L1_sign'],
        ['L2_acct', 'L2_sign'],
        ['L3_acct', 'L3_sign'],
        ['L4_acct', 'L4_sign'],
        ['L5_acct', 'L5_sign'],
        ['L6_acct', 'L6_sign'],
    ]

    tempdataC = []
    for col_name in col_names:
        tempdata = rawdata.loc[rawdata[col_name[0]] != 0]
        tempsign = tempdata[col_name[1]]
        tempdata = tempdata.assign(
            value=tempdata.value*tempsign).groupby(['period','financials_type', col_name[0]]).value.sum()
        tempdata = tempdata.reset_index()
        tempdata.columns = headerCol
        tempdataC.append(tempdata)

    data = dataL0.append(tempdataC)
    data = pd.merge(data, acctRef[{
        'account_gl', 'prefix', 'sort_order', 'account_caption', 'html_class'}], on='account_gl')
    
    data['quarter'] = pd.to_datetime(data['period']).dt.quarter
    data['annual'] = pd.to_datetime(data['period']).dt.year

    return data


def pivotCF(data, acctRef, column):

    dataPivotM = pd.pivot_table(
        data,
        values="value",
        index=["account_gl"],
        columns=column,
        aggfunc="sum"
    )

    dataPivotM = dataPivotM.reset_index()
    result = pd.merge(dataPivotM, acctRef[{
        'account_gl', 'prefix', 'sort_order', 'account_caption', 'html_class'}], on='account_gl')

    cols = result.columns.tolist()
    cols = cols[-1:] + cols[:-1]
    result = result[cols]
    result.sort_values(by=['prefix', 'sort_order'])
    result = result[result.columns[~result.columns.isin(
        ['account_gl', 'prefix', 'sort_order'])]]
    result = result.fillna(0)

    return result
