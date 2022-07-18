from datetime import datetime

from models import OverallValidateRecordModel
from utils.validate import send_to_slack
import pydash


class BaseValidateOverall:
    task_name = ''
    project = ''
    business_type = ''
    date_str = ''
    chain = ''
    config = {}
    # 所有检验报告
    all_report = []
    # 整体结果报告
    overall_report = {
        'result_code': 0,
        'result_message': '校验通过',
        'success': [],
        'fail': [],
        'score': 0,
        # {table_name:count}
        'validate_table_cover': {},
        'has_warn': False
    }

    def basic(self):
        return []

    def inner(self):
        return []

    def outside(self):
        return []

    def merge_cfg(self, config1, config2):
        return pydash.assign(config1, config2)

    def __init__(self, config) -> None:
        self.config = self.merge_cfg(pydash.clone_deep(config), pydash.clone_deep(self.config))
        self.chain = self.config['chain']
        self.project = self.config['project']
        if 'date_str' in self.config:
            self.date_str = self.config['date_str']
        self.task_name = '{}_{}_overall'.format(self.business_type, self.project)
        self.overall_report['project'] = self.project
        self.overall_report['args'] = self.config
        self.overall_report['business_type'] = self.business_type

    def validate_all(self):
        '''
        校验所有规则
        :return: 每个规则的报告
        '''
        self.overall_report['start_time'] = datetime.now()

        # 核心执行逻辑
        all_validate = self.get_all_validate()
        # TODO 多线程优化速度
        for v in all_validate:
            # 初始化并执行
            r = v['cls'](self.merge_cfg(v['config'], self.config)).validate()
            self.all_report.append(r)

        self.overall_report['end_time'] = datetime.now()
        self.overall_report['execute_time'] = (self.overall_report['end_time'] - self.overall_report[
            'start_time']).total_seconds() * 1000

    def get_all_validate(self):
        ls = []
        ls.extend(self.basic())
        ls.extend(self.outside())
        ls.extend(self.inner())
        return ls

    def add_table_cover(self, tables):
        for table in tables:
            if table not in self.overall_report['validate_table_cover']:
                self.overall_report['validate_table_cover'][table] = 0
            self.overall_report['validate_table_cover'][table] += 1

    def gen_report(self):
        '''
        生成整体报告
        :return:
        '''
        # verify_table_cover = {}
        for report in self.all_report:
            if report['result_code'] == 0:
                self.overall_report['success'].append({
                    '_id': report['_id'],
                    'validate_name': report['validate_name']
                })
                # 只有校验通过的才算覆盖
                self.add_table_cover(report['validate_table'])
                # TODO 计算信心分
                # self.overall_report['score'] += report['score']
            else:
                self.overall_report['fail'].append({
                    '_id': report['_id'],
                    'validate_name': report['validate_name']
                })

        if len(self.overall_report['fail']) > 0:
            self.overall_report['result_code'] = 1

        self.overall_report['result_message'] = 'business_type:{} project:{} 校验情况{}/{}'.format(
            self.business_type,
            self.project,
            len(self.overall_report['success']),
            len(self.all_report)
        )

        # 保存到DB
        res = OverallValidateRecordModel.insert_one(self.overall_report)
        self.overall_report['_id'] = pydash.get(res, 'inserted_id')

        print('{} 记录:{}'.format(self.overall_report['result_message'], self.overall_report['_id']))
        # TODO 把错误都print出来

    def push_overall_report_to_slack(self):
        # 推送到slack
        send_to_slack(self.overall_report['result_message'])
        # 更新DB发送状态
        query = {'_id': self.overall_report['_id']}
        update = {'has_warn': True}
        OverallValidateRecordModel.update_one(query, update)

    def process(self):
        self.validate_all()
        self.gen_report()
        # self.push_overall_report_to_slack()
