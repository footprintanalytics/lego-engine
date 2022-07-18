
from ..base_validate import BaseValidate


class BaseBasicValidate(BaseValidate):

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
        return {}

    # 检查内部数据
    def check_data(self, self_data: dict):
        result = self.check_rule(self_data)
        return result
