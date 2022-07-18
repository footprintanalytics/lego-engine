from validation.basic_validate.data_continuity.data_continuity_validate import DataContinuityValidate
import pydash


class DataContinuityTokenPricePre5MinuteValidate(DataContinuityValidate):
    validate_name = 'data_continuity_token_price_pre_5_minute_validate'
    desc = '币价的数据的连续性校验'
    validate_target = 'tokenPrice'
    slack_warn = True
    project = 'all'
    need_warn_project_list = ['all']

    def get_self_sql(self):
        return """
        select day,count(address) as count from (
            select day,address from (
                select address,DATE(timestamp) AS day from `footprint-etl-internal.view_to_table.fixed_price`
            ) group by day,address
        ) group by day
        having  (day < '2021-09-15' AND count < 1000) or (day >= '2021-09-15' AND count < 3000)
        """

    def check_data(self, self_data: dict):
        data = pydash.get(self_data, 'data', [])
        miss_date_info = pydash.get(data, '[0]', None)
        return {
            "result_code": 0 if len(data) == 0 else 1,
            "result_message": '校验成功' if len(data) == 0 else '校验失败',
            "validate_data": {
                "validate_field": self.validate_target,
                "validate_table": self.validate_table,
                "validate_count": len(data),
                "miss_date_info": miss_date_info,
                "sql": pydash.get(self_data, 'sql', '')
            }
        }


if __name__ == '__main__':
    v = DataContinuityTokenPricePre5MinuteValidate({})
    v.validate()
