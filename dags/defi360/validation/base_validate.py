import moment
from datetime import datetime
from utils.validate import save_monitor
import pydash


class BaseValidate:
    # 校验类型 basic / inside / external
    validate_type: str = None
    # 校验名
    validate_name: str = None
    # 校验阈值
    validate_args: dict = {}
    # 校验时间
    validate_date: str = None
    # 检验的表
    validate_table: list = []
    # 监控规则描述
    desc: str = None
    # 是否报警
    slack_warn: bool = False
    # 链
    chain: str = None
    # 平台
    project: str = None
    # 是否开启校验
    switch_validate = True
    # data frame 转换格式
    data_frame_orient = 'dict'

    def __init__(self, cfg):
        chain = pydash.get(cfg, 'chain')
        project = pydash.get(cfg, 'project')
        validate_date = pydash.get(cfg, 'validate_date')
        validate_table = pydash.get(cfg, 'validate_table', [])
        self.validate_args = pydash.assign(self.validate_args, cfg)

        if not self.validate_date:
            self.validate_date = moment.now().add(days=-1).format('YYYY-MM-DD')
        if not self.project:
            self.project = project
        if not self.chain:
            self.chain = chain
        if not self.validate_table:
            self.validate_table = validate_table
        if self.validate_name is None:
            self.validate_name = self.__class__.__name__

    def add_validate_table(self, table):
        if table not in self.validate_table:
            self.validate_table.append(table)

    def validate(self):
        if not self.switch_validate:
            return

        print(f'开始执行: {self.__class__.__name__} cfg:{self.validate_args}')
        # 校验起始时间
        stats_start = datetime.now()
        # 各个校验方式的校验规则
        # { code: 0 成功 1 失败，message：结果说明，validate_data: dict }
        result = self.validate_process()
        print('validate result: ', result)
        # 校验结束时间
        stats_end = datetime.now()
        execute_time = datetime.timestamp(stats_end) - datetime.timestamp(stats_start)

        time_result = {
            "stats_start": stats_start,
            "stats_end": stats_end,
            "execute_time": execute_time
        }

        if result["result_code"] == 1:
            print(f'{self.validate_name}校验失败，{result["result_message"]}')
        res = self.save_db(result, time_result)
        # 统一结果
        # return res

    # 不同校验规则主要实现这个方法
    def validate_process(self):
        return {
            "result_code": 0,
            "result_message": '基类校验成功',
            "validate_data": {}
        }

    def save_db(self, result: dict, time_result: dict):
        validate_data = {
            "project": self.project,
            "chain": self.chain,
            "validate_type": self.validate_type,
            "validate_name": self.validate_name,
            "validate_date": self.validate_date,
            "validate_args": self.validate_args,
            "validate_table": self.validate_table,
            "desc": self.desc,
            "stats_start": time_result.get("stats_start"),
            "stats_end": time_result.get("stats_end"),
            "execute_time": time_result.get("execute_time"),
            "middle_output": result.get("validate_data"),
            "result_code": result.get("result_code"),
            "result_message": result.get("result_message"),
            "has_warn": False
        }
        slack_warn = self.slack_warn
        error = pydash.get(result, 'validate_data.error', '')
        if error:
            slack_warn = False
        return save_monitor(validate_data, slack_warn)
