import os
from string import Template

import moment

from utils.constant import PROJECT_PATH
from utils.template.tempate_gen import BaseTemplate


class FarmingTemplate(BaseTemplate):
    temp_type = 'farming'
    tpl_file_path = '{}/utils/template/farming/farming_template.txt'.format(PROJECT_PATH)
    init_file_path = '{}/utils/template/farming/init_template.txt'.format(PROJECT_PATH)
    type_list = ['supply', 'withdraw', 'reward']

    def __init__(self, project, chain='ethereum') -> None:
        super().__init__(project)
        self.chain = chain
        self.project_path = f'{PROJECT_PATH}/{self.temp_type}/{chain}/{project}'

    def gen_template(self):
        self.gen_folder()  # 创建文件
        self.gen_swap_sql()  # 创建swap.sql
        print('done')
        pass

    def gen_folder(self):
        if not os.path.exists(self.project_path):
            os.makedirs(self.project_path)

        for type_ in self.type_list:
            template_file_path = os.path.join(self.project_path, f'{self.project}_{self.temp_type}_{type_}.py')
            tpl_file = open(self.tpl_file_path)
            g_file = open(template_file_path, 'w')
            lines = []
            tpl = Template(tpl_file.read())
            lines.append(
                tpl.substitute(project=self.project, project_up=self.project.capitalize(), type=type_,
                               type_up=type_.capitalize(),
                               chain=self.chain,
                               date="'{}'".format(moment.utcnow().datetime.strftime("%Y-%m-%d"))))
            g_file.writelines(lines)
            tpl_file.close()
            g_file.close()
        init_template_file_path = os.path.join(self.project_path, '__init__.py')
        tpl_file = open(self.init_file_path)
        g_file = open(init_template_file_path, 'w')
        lines = []
        tpl = Template(tpl_file.read())
        s_type, w_type, r_type = 'supply',  'withdraw', 'reward'
        lines.append(
            tpl.substitute(project=self.project, project_up=self.project.capitalize(),
                           chain=self.chain,
                           s_type=s_type, s_type_up=s_type.capitalize(), r_type=r_type, r_type_up=r_type.capitalize(),
                           w_type=w_type, w_type_up=w_type.capitalize()))
        g_file.writelines(lines)
        tpl_file.close()
        g_file.close()

    def gen_swap_sql(self):
        for type_ in self.type_list:
            sql = '''
                SELECT
                project,
                version,
                protocol_id,
                type,
                block_number,
                block_timestamp,
                transaction_hash,
                log_index,
                contract_address,
                operator,
                asset_address,
                asset_amount
                from view.{}
                where Date(block_timestamp) {{match_date_filter}}
            '''.format(type_)
            out_file_path = os.path.join(self.project_path, f'{self.project}_{self.temp_type}_{type_}.sql')
            out_file = open(out_file_path, 'w')
            out_file.write(sql)
            out_file.close()


if __name__ == '__main__':
    """
    project 必填, 平台名称
    chain 可选, ethereum/bsc 默认值 ethereum
    """
    t = FarmingTemplate('venus', chain='bsc')
    t.gen_template()
