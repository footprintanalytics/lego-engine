from .base_basic_validate import BaseBasicValidate
import pydash
from utils.query_bigquery import query_bigquery
from datetime import datetime, timedelta
import moment


class DataContinuityValidate(BaseBasicValidate):
    validate_type = 'basic'
    validate_name = 'DeFi360_data_continuity'
    slack_warn = True
    data_frame_orient = 'record'
    desc = '流水连续性检测'

    def __init__(self, cfg):
        super().__init__(cfg)
        validate_table = cfg["validate_table"]
        self.validate_table = [validate_table]

        self.since_date = (datetime.strptime(self.validate_date, '%Y-%m-%d') + timedelta(days=-7)).strftime('%Y-%m-%d')
        self.date = "between '{}' and '{}'".format(self.since_date, self.validate_date)

    def get_self_sql(self) -> str:
        return """
            SELECT
                *,
                CONCAT(DATE_ADD(day,INTERVAL 1 day),' ~ ', DATE_ADD(next_day,INTERVAL -1 day)) AS loss_date_data
            FROM (
                SELECT
                    business_type,
                    protocol_id,
                    DATE(block_timestamp) AS day,
                    LEAD(DATE(block_timestamp), 1, DATE(CURRENT_TIMESTAMP())) OVER (PARTITION BY business_type ORDER BY DATE(block_timestamp)) AS next_day,
                    block_timestamp
                FROM
                    `{table}`
                WHERE
                    Date(block_timestamp) {date}
                ORDER BY
                    block_timestamp DESC
            )
            WHERE
                DATE_DIFF(next_day,day,day) > 1
        """.format(
            table=self.validate_table[0],
            date=self.date
        )

    def check_data(self, self_data: dict):
        data = pydash.get(self_data, 'data', [])
        miss_date_info = list(
            map(
                lambda n: {
                    'protocol_id': pydash.get(n, 'protocol_id'),
                    'loss_date_data': pydash.get(n, 'loss_date_data'),
                    'business_type': pydash.get(n, 'business_type')
                },
                data
            )
        )
        error = pydash.get(self_data, 'error', '')

        return {
            "result_code": 0 if len(data) == 0 and error == '' else 1,
            "result_message": '连续性校验成功' if len(data) == 0 and error == '' else '连续性校验失败',
            "validate_data": {
                "validate_table": self.validate_table,
                "validate_count": len(data),
                "miss_date_info": miss_date_info,
                "error": error,
                "sql": pydash.get(self_data, 'sql', 0)
            }
        }
