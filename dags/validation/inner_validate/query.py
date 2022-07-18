from utils.query_bigquery import query_bigquery


def lending_asset_sum(chain, project, business_table, business_type='lending'):
    '''
      获取资产的总和
      :param chain:
      :param project:
      :param business_type:
      :return:
      '''
    sql = """SELECT token_address,SUM(token_amount_raw) as amount FROM footprint-etl.{chain}_{business_type}_{project}.{project}_{business_type}_{business_table}_all GROUP BY token_address""".format(
        chain=chain, business_type=business_type, project=project, business_table=business_table)
    data = query_bigquery(sql)
    return data.to_dict(orient='records')


def lending_liquidation_collateral(chain, project):
    sql = """SELECT token_collateral_address as token_address,SUM(token_collateral_amount_raw) as amount FROM footprint-etl.{chain}_{business_type}_{project}.{project}_{business_type}_{business_table}_all GROUP BY token_collateral_address""".format(
        chain=chain, business_type='lending', project=project, business_table='liquidation')
    data = query_bigquery(sql)
    return data.to_dict(orient='records')


def lending_liquidation_repay(chain, project):
    sql = """SELECT repay_token_address as token_address,SUM(repay_token_amount_raw) as amount FROM footprint-etl.{chain}_{business_type}_{project}.{project}_{business_type}_{business_table}_all GROUP BY repay_token_address""".format(
        chain=chain, business_type='lending', project=project, business_table='liquidation')
    data = query_bigquery(sql)
    return data.to_dict(orient='records')


def group_by_asset(data):
    result = {
    }
    for item in data:
        for detail in item['data']:
            if detail['token_address'] not in result:
                result[detail['token_address']] = {}
            result[detail['token_address']][item['name']] = detail['amount']
    return result


def lending_pool_asset_sum(chain, project, business_table, business_type='lending'):
    '''
      获取池子资产的总和
      :param chain:
      :param project:
      :param business_type:
      :return:
      '''
    sql = """SELECT contract_address,token_address,SUM(token_amount_raw) as amount FROM footprint-etl.{chain}_{business_type}_{project}.{project}_{business_type}_{business_table}_all GROUP BY token_address,contract_address""".format(
        chain=chain, business_type=business_type, project=project, business_table=business_table)
    data = query_bigquery(sql)
    return data.to_dict(orient='records')


def lending_pool_liquidation_repay(chain, project):
    sql = """SELECT contract_address,repay_token_address as token_address,sum(repay_token_amount_raw) as amount, FROM footprint-etl.{chain}_lending_{project}.{project}_lending_liquidation_all group by contract_address,repay_token_address""".format(
        chain=chain, project=project)
    data = query_bigquery(sql)
    return data.to_dict(orient='records')


def lending_pool_liquidation_collateral(chain, project):
    sql = """SELECT contract_address,token_collateral_address as token_address,sum(token_collateral_amount_raw) as amount, FROM footprint-etl.{chain}_lending_{project}.{project}_lending_liquidation_all group by contract_address,token_collateral_address""".format(
        chain=chain, project=project)
    data = query_bigquery(sql)
    return data.to_dict(orient='records')


def group_by_pool_asset(data):
    result = {}
    for item in data:
        for detail in item['data']:
            if detail['contract_address'] not in result:
                result[detail['contract_address']] = {}
            if detail['token_address'] not in result[detail['contract_address']]:
                result[detail['contract_address']][detail['token_address']] = {}
            result[detail['contract_address']][detail['token_address']][item['name']] = detail['amount']
    return result
