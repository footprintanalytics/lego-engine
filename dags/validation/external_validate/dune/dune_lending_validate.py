import os

import pandas as pd

from utils.constant import PROJECT_PATH
from validation.external_validate.base_external_validate import BaseExternalValidate

dune_lending_projects = ['Aave', 'Compound', 'MakerDAO']


class DuneLendingValidate(BaseExternalValidate):
    validate_name = 'dex_dex_trades_count_validate'
    desc = '交易数量校验：(external_value-self_value)/self_value > difference_rate'
    validate_args = {'difference_rate': 0.1}
    validate_target = 'trades_count'

    def get_external_data(self):
        if self.project not in dune_lending_projects:
            raise Exception(self.project + ' is not dune lending project')
        df = pd.read_csv(os.path.join(PROJECT_PATH, 'validation/external_validate/dune/dune_lending_2021_10_all.csv'))
        return df.loc[df['project'] == self.project]

    # 获取内部对比数据
    def get_self_data(self):
        # sql_path = os.path.join(PROJECT_PATH, 'validation/external_validate/dune/lending_aggregate.sql')
        # sql = read_file(sql_path)
        # sql = sql.format(project=self.project.lower())
        # print('get_self_data query_string', sql)
        # df = query_bigquery(sql, self.project_id)
        # df = df.to_csv(os.path.join(PROJECT_PATH, 'validation/external_validate/dune/dune_lending_debug.csv'))
        df = pd.read_csv(os.path.join(PROJECT_PATH, 'validation/external_validate/dune/dune_lending_debug.csv'))
        return df

    def cal_rate(self, item):
        item['nums_rate'] = abs(item['nums_x'] - item['nums_y']) / item['nums_y']
        item['token_amount_rate'] = abs(item['token_amount_x'] - item['token_amount_y']) / item['token_amount_y']
        return item

    def check_data(self, external_data, self_data):
        df = pd.merge(external_data, self_data, how='left', on=['date', 'project', 'version', 'type'])
        df = df.apply(self.cal_rate, axis=1)
        # print('===> merge_data ', df)
        nums_diff = df.loc[df['nums_rate'] >= self.validate_args.get('difference_rate')]
        token_amount_diff = df.loc[df['token_amount_rate'] >= self.validate_args.get('difference_rate')]
        # print('===> diff ', nums_diff)
        nums_diff_list = nums_diff.to_dict(orient='records')
        token_amount_list = token_amount_diff.to_dict(orient='records')
        result = {
            'result_code': len(nums_diff_list) == 0 and len(token_amount_list) == 0,
            'validate_data': {
                'validate_target': self.validate_target,
                'nums_diff': nums_diff_list,
                'token_amount_diff': token_amount_list
            },
            'result_message': ''
        }
        print('===> result ', result)
        return result


if __name__ == '__main__':
    DuneLendingValidate(
        cfg={'chain': 'ethereum', 'project': 'Compound', 'protocol_id': 10}).validate()
