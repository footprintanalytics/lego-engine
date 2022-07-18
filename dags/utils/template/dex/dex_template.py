import base64
import os
from string import Template

import moment
import requests

from utils.constant import PROJECT_PATH
from utils.template.dex.dex_sql_transfer import SqlTransfer
from utils.template.tempate_gen import BaseTemplate


class DexTemplate(BaseTemplate):
    dune_dex_folder = 'https://api.github.com/repos/duneanalytics/abstractions/contents/ethereum/dex/trades/'
    tpl_file_path = '{}/utils/template/dex/dex_template.txt'.format(PROJECT_PATH)
    temp_type = 'dex'

    def gen_template(self):
        self.gen_folder()  # 创建文件
        self.gen_swap_sql()  # 创建swap.sql
        self.gen_liquidity_sql() # 创建liquidity相关文件
        print('done')
        pass

    def gen_folder(self):
        if os.path.exists(self.project_path):
            print('project:{} 已存在目录:{}'.format(self.project, self.project_path))
            return

        os.makedirs(self.project_path)
        template_file_path = os.path.join(self.project_path, '{}.py'.format(self.project))
        tpl_file = open(self.tpl_file_path)
        g_file = open(template_file_path, 'w')
        lines = []
        tpl = Template(tpl_file.read())
        lines.append(tpl.substitute(
            project=self.project,
            project_capitalize=self.project.capitalize(),
            chain=self.chain,
            date="'{}'".format(moment.utcnow().datetime.strftime("%Y-%m-%d"))))
        g_file.writelines(lines)
        tpl_file.close()
        g_file.close()

    # def gen_swap_sql(self):
    #     swap_sql = self.project_path + '/swap.sql'
    #     if os.path.exists(swap_sql):
    #         print('project:{} 已存在文件:{}'.format(self.project, swap_sql))
    #         return
    #
    #     sql_file_name = 'insert_%s.sql' % self.project
    #     dune_sql_url = os.path.join(self.dune_dex_folder, sql_file_name.lower())
    #     req = requests.get(dune_sql_url)
    #     req = req.json()
    #     content = base64.b64decode(req['content']).decode('utf-8')
    #     res_sql = SqlTransfer(content, self.project).process()
    #     swap_out_file_path = os.path.join(self.project_path, 'swap.sql')
    #     swap_out_file = open(swap_out_file_path, 'w')
    #     swap_out_file.write(res_sql)
    #     swap_out_file.close()

    def gen_liquidity_sql(self):
        add_liquidity_path = self.project_path + f'/{self.project}_add_liquidity.sql'
        remove_liquidity_path = self.project_path + f'/{self.project}_remove_liquidity.sql'
        if os.path.exists(add_liquidity_path):
            print('project:{} 已存在文件:{}'.format(self.project, add_liquidity_path))
            pass

        add_liquidity_file = open(add_liquidity_path, 'w')
        add_liquidity_file.close()
        remove_liquidity_file = open(remove_liquidity_path, 'w')
        remove_liquidity_file.close()

    def gen_swap_sql(self):
        sql = '''
            SELECT
            project,
            version,
            protocol_id,
            'DEX' as category,
            block_number,
            block_time,
            tx_hash,
            log_index,
            contract_address,
            trader_a,
            NULL AS trader_b,
            token_a_amount_raw,
            token_b_amount_raw,
            NULL AS usd_amount,
            token_a_address,
            token_b_address,
            exchange_contract_address,
            NULL AS trace_address,
            from view.{}
            where Date(block_timestamp) {match_date_filter}
        '''
        out_file_path = os.path.join(self.project_path, 'swap.sql')
        out_file = open(out_file_path, 'w')
        out_file.write(sql)
        out_file.close()



if __name__ == '__main__':
    t = DexTemplate('pangolin', 'avalanche')
    t.gen_template()
