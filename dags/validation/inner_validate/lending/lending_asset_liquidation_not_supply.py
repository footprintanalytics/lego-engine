from utils.query_bigquery import query_bigquery
from validation.inner_validate.base_inner_validate import BaseInnerValidate
import pydash

from validation.inner_validate.query import lending_asset_sum, group_by_asset


class LendingAssetLiquidationNotSupply(BaseInnerValidate):

    def __init__(self, cfg):
        super().__init__(cfg)
        for t in ['borrow', 'repay', 'supply', 'withdraw']:
            self.add_validate_table(
                'footprint-etl.{chain}_{business_type}_{project}.{project}_{business_type}_{t}_all'.format(
                    chain=self.chain, project=self.project, business_type=self.business_type, t=t))

    def get_self_data(self):
        sql = """
        SELECT DISTINCT(token_collateral_address) AS token_address
        FROM
          `footprint-etl.{chain}_{business_type}_{project}.{project}_{business_type}_liquidation_all` liquidation
        LEFT JOIN
          `footprint-etl.{chain}_{business_type}_{project}.{project}_{business_type}_supply_all` supply
        ON supply.token_address = liquidation.token_collateral_address WHERE supply.token_address IS NULL
        """.format(
            chain=self.chain, business_type=self.business_type, project=self.project)
        data = query_bigquery(sql)
        return data.to_dict(orient='records')

    def check_rule(self, values):
        result_code = 0
        result_message = '清算资产必须有存入记录校验成功'
        data_map = {}
        if len(values) > 0:
            result_code = 1
            result_message = f'清算资产必须有存入记录校验失败，错误的token:{values[0]["token_address"]}'
            data_map = values
        return {
            "result_code": result_code,
            "result_message": result_message,
            "validate_data": {
                'query_data': data_map
            }
        }


if __name__ == '__main__':
    v = LendingAssetLiquidationNotSupply({
        "chain": "ethereum",
        "business_type": "lending",
        "project": "compound",
    })

    v.validate()
