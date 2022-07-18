from ..base_validate import BaseValidate


class BaseInnerValidate(BaseValidate):
    validate_type = 'inner'

    # 开始校验
    def validate_process(self):
        self_data = self.get_self_data()
        result = self.check_data(self_data)

        return result

    # 校验规则
    def check_rule(self, values: dict):
        return {
            "result_code": 0,
            "result_message": '内部校验基类成功',
            "validate_data": {}
        }

    # 获取内部数据
    def get_self_data(self):
        return {}

    # 检查内部数据
    def check_data(self, self_data: dict):
        result = self.check_rule(self_data)
        return result

    def messsage(self, verify_result, fail_message: str = None, ex_data={}):
        return {
            "result_code": 0 if verify_result else 1,
            "result_message": '校验成功' if verify_result else fail_message,
            "validate_data": {
                **self._change_to_str(ex_data)
            }
        }

    def _change_to_str(self, data: dict) -> dict:
        return {key: str(value) for key, value in data.items()}
