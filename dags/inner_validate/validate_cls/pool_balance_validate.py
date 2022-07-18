from inner_validate.validate_cls.base_validate import BaseValidate
from utils.query_bigquery import query_bigquery
import pydash


class PoolBalanceValidate(BaseValidate):
    valid_rule_name = 'POOL_TOKEN_BALANCE'
    percent = 0.85

    def validate_result(self, dex_sql):
        df_dex = query_bigquery(dex_sql)
        result = df_dex.to_dict(orient='dict')

        try:
            print(result)
            total = int(pydash.get(result, 'count.0', 0)) + int(pydash.get(result, 'count.1', 0))
            true_count = int(pydash.get(result, 'count.0')) if pydash.get(result, 'result.0') else int(pydash.get(result, 'count.1'))

            return {
                "result": (true_count / (total or 1)) > self.percent,
                "value": true_count / total
            }
        except Exception as e:
            print(e)
            return False
