from validation.basic_validate.base_basic_validate import BaseBasicValidate
import pydash
from utils.query_bigquery import query_bigquery
from datetime import datetime, timedelta


class DataContinuityValidate(BaseBasicValidate):
    validate_type = 'basic'
    slack_warn = True
    validate_args = {}
    validate_target = ''
    need_warn_project_list = ['curve', 'pancakeswap','uniswap','sushi','balancer','bancor','mdex','quickswap','biswap','elipsis','pangolin','traderjoe']

    def __init__(self, cfg):
        super().__init__(cfg)
        if pydash.get(cfg, 'validate_date'):
            self.validate_date = pydash.get(cfg, 'validate_date')
        self.since_date = (datetime.strptime(self.validate_date, '%Y-%m-%d') + timedelta(days=-90)).strftime('%Y-%m-%d')
        self.date = "between '{}' and '{}'".format(self.since_date, self.validate_date)
        self.interval_days = pydash.get(cfg, 'interval_days',1)

    def get_self_sql(self) -> str:
        raise Exception('必须改写该方法')

    def get_self_data(self):
        sql = self.get_self_sql()
        df = query_bigquery(sql, project_id='footprint-etl-internal')
        return {
            "data": df.to_dict(orient='record'),
            "sql": sql
        }

    def check_data(self, self_data: dict):
        data = pydash.get(self_data, 'data', [])
        miss_date_info = list(map(lambda n: {'protocol_id': int(pydash.get(n, 'protocol_id')),
                                             'loss_date_data': pydash.get(n, 'loss_date_data')}, data))
        print(miss_date_info)
        return {
            "result_code": 0 if len(data) == 0 else 1,
            "result_message": '校验成功' if len(data) == 0 else '校验失败',
            "validate_data": {
                "validate_field": self.validate_target,
                "validate_table": self.validate_table,
                "validate_count": len(data),
                "miss_date_info":miss_date_info,
                "sql": pydash.get(self_data, 'sql', 0)
            }
        }
