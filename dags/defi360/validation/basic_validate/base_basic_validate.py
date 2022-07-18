
from ..base_validate import BaseValidate
from utils.query_bigquery import query_bigquery
import pydash


class BaseBasicValidate(BaseValidate):
    validate_field = []

    def __init__(self, cfg):
        super().__init__(cfg)
        validate_field = cfg["validate_field"]
        validate_table = cfg["validate_table"]

        self.validate_field = validate_field
        self.validate_table = [validate_table]

    # 开始校验
    def validate_process(self):
        self_data = self.get_self_data()
        result = self.check_data(self_data)

        return result

    # 校验规则
    def check_rule(self, values: dict):
        return {
            "result_code": 0,
            "result_message": '基础校验基类成功',
            "validate_data": {}
        }

    # 获取内部数据
    def get_self_data(self):
        sql = self.get_self_sql()
        data = {}
        error = ''
        try:
            data = query_bigquery(sql)
            data = data.to_dict(orient=self.data_frame_orient)
        except Exception as e:
            print(
                '{project} {validate_name} query error'.format(
                    validate_name=self.validate_name,
                    project=self.project
                )
            )
            error = str(e)
        return {
            "data": data,
            "sql": sql,
            "error": error
        }

    def get_self_sql(self):
        return ''

    # 检查内部数据
    def check_data(self, self_data: dict):
        invalidate_count = pydash.get(self_data, 'data.count.0', 0)
        error = pydash.get(self_data, 'error', '')

        return {
            "result_code": 0 if invalidate_count == 0 and error == '' else 1,
            "result_message": f'{self.validate_name} 校验成功' if invalidate_count == 0 and error == '' else f'{self.validate_name} 校验失败',
            "validate_data": {
                "validate_field": self.validate_field,
                "validate_table": self.validate_table,
                "invalidate_count": invalidate_count,
                "sql": pydash.get(self_data, 'sql', 0),
                "error": error,
            }
        }
