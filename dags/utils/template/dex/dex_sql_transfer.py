import re
import sql_metadata
import pandas
import os
from utils.constant import PROJECT_PATH



class SqlTransfer:
    sql: str
    project: str

    def __init__(self, sql, project) -> None:
        self.sql = sql
        self.project = project

    def extract_core(self):
        start = self.sql.find('SELECT')
        end = self.sql.rfind('ON CONFLICT DO NOTHING')
        # start 和end 如果是-1则错误
        self.sql = self.sql[start: end]

    def remove_type_define(self):
        pattern = re.compile('::[a-z,\[\]]+\s')
        self.sql = re.sub(pattern, u' ', self.sql)

    def replace_array(self):
        match = re.findall(re.compile('\".+\"\[[0-9]+\]'), self.sql)
        for origin in match:
            column_str = re.findall(re.compile('\".+\"'), origin)[0].replace('"', '')
            array_index = re.findall(re.compile('\[[0-9]+\]'), origin)[0].replace('[', '').replace(']', '')
            self.sql = self.sql.replace(origin,
                                        f'{column_str}[safe_ORDINAL({array_index})]'.format(column_str=column_str,
                                                                                            array_index=array_index))

    def fix_usd_amount(self):
        self.sql = self.sql.replace("""coalesce(
                usd_amount,
                token_a_amount_raw / 10 ^ pa.decimals * pa.price,
                token_b_amount_raw / 10 ^ pb.decimals * pb.price
            ) as usd_amount,""", """NULL as usd_amount,""")

    def remove_token(self):
        self.sql = self.sql.replace('token_a_amount_raw / 10 ^ erc20a.decimals AS token_a_amount,\n', '')
        self.sql = self.sql.replace('token_b_amount_raw / 10 ^ erc20b.decimals AS token_b_amount,\n', '')
        match = re.findall(re.compile('.*\serc20[ab].*\n'), self.sql)
        for t in match:
            self.sql = self.sql.replace(t, '')

    def remove_price_table(self):
        return self.sql.replace("""    LEFT JOIN prices.usd pa ON pa.minute = date_trunc('minute', dexs.block_time)
            AND pa.contract_address = dexs.token_a_address
            AND pa.minute >= start_ts
            AND pa.minute < end_ts
        LEFT JOIN prices.usd pb ON pb.minute = date_trunc('minute', dexs.block_time)
            AND pb.contract_address = dexs.token_b_address
            AND pb.minute >= start_ts
            AND pb.minute < end_ts""", '')

    def replace_columns(self):
        self.sql = self.sql.replace('"', '')
        # 处理 ::xxx 的错误
        self.sql = self.sql.replace('::numeric', '')

        # sql = sql.replace('0xEeeeeEeeeEeEeeEeEeEeeEEEeeeeEeeeeeeeEEeE', '0x0000000000000000000000000000000000000000')

        # 处理 \x 地址的错误
        match = re.findall(re.compile("\'\\\\x.{40}\'"), self.sql)
        for t in match:
            self.sql = self.sql.replace(t, t.replace('\\x', '0x'))
        all_tables_aliases = list(sql_metadata.Parser(self.sql).tables_aliases)

        columns_mapping = {
            'evt_tx_hash': 'transaction_hash',
            # 'evt_index': 'log_index',
            # 下面两个单独处理
            # 'block_time': 'block_timestamp',
            # 'evt_block_time': 'block_timestamp',
        }
        # 引用替换
        for column in columns_mapping:
            # 先替换别名的
            match_re = ['.+\.{}\sAS\s.*\n', '\,\s{}\,', '\.{}\,.*\n']
            for m in match_re:
                match = re.findall(re.compile(m.format(column)), self.sql)
                for origin_text in match:
                    if origin_text.find('.') == -1 or origin_text.strip().split('.')[0] in all_tables_aliases:
                        self.sql = self.sql.replace(origin_text, origin_text.replace(column, columns_mapping[column]))

            match_re = ['.+\.{}\s.*\n', '{}\,.*\n']
            for m in match_re:
                match = re.findall(re.compile(m.format(column)), self.sql)
                for origin_text in match:
                    if origin_text.find('.') == -1 or origin_text.strip().split('.')[0] in all_tables_aliases:
                        self.sql = self.sql.replace(origin_text,
                                                    origin_text.replace(column,
                                                                        '{} AS {}'.format(columns_mapping[column],
                                                                                          column)))
        self.sql = self.sql.replace('t.evt_block_time AS block_time,\n', 't.block_timestamp AS block_time,\n')
        self.sql = self.sql.replace('t.evt_index\n', 't.log_index AS evt_index\n')
        self.sql = self.sql.replace('evt_block_time as block_time,\n', 'block_timestamp as block_time,\n')
        self.sql = self.sql.replace('evt_tx_hash as tx_hash,\n', 'transaction_hash as tx_hash,\n')
        self.sql = self.sql.replace('evt_index\n', 'log_index AS evt_index\n')

    def remove_tx(self):
        all_tables_aliases = sql_metadata.Parser(self.sql).tables_aliases
        alia = None
        for a in all_tables_aliases:
            if all_tables_aliases[a] == 'ethereum.transactions':
                alia = a
        if alia is None:
            return
        match = re.findall(re.compile('.+{}\..+\n'.format(alia)), self.sql)
        match.extend(re.findall(re.compile('.*ethereum\.transactions.*\n'), self.sql))
        match.extend(re.findall(re.compile('.*start\_ts.*\n'), self.sql))
        match.extend(re.findall(re.compile('.*end\_ts.*\n'), self.sql))
        for t in match:
            self.sql = self.sql.replace(t, '')

    def get_nickname(self, name: str) -> str:
        name_split = name.split('.')
        nickname = name_split[0]
        for index in range(len(name_split)):
            if index > 0:
                nickname += '."{}"'.format(name_split[index])
        return nickname

    def replace_table(self):
        # TODO 直接读取BigQuery线上表
        all_tables = pandas.read_csv(os.path.join(PROJECT_PATH, 'utils/bq_table.csv'))

        table_mapping = {
            # 'erc20.tokens': 'xed-project-237404.footprint_etl.erc20_tokens',
            # 'ethereum.transactions': 'bigquery-public-data.crypto_ethereum.transactions'
        }
        project_bigquery_tables = all_tables[all_tables['table_schema'].str.contains(self.project)]
        # sql 需要的tables airswap.swap_evt_Swap
        tables = sql_metadata.Parser(self.sql).tables
        for table in tables:
            if table == 'ethereum.transactions':
                continue
            # 处理引用语法错误 例如 sushi."swap" -> sushi.swap
            self.sql = self.sql.replace(self.get_nickname(table), table)
            # 优先按映射表匹配
            if table in table_mapping:
                self.sql = self.sql.replace(' {} '.format(table), ' {} '.format(table_mapping[table]))
            # 如果是项目特有的表，根据相似度匹配
            elif self.project in table:
                format_table_name = table.replace('{}.'.format(self.project), '').replace('_evt_', '_event_')
                match_table_df = project_bigquery_tables[
                    project_bigquery_tables['table_name'].str.contains(format_table_name, case=False)]
                if len(match_table_df) > 1:
                    raise Exception('table替换失败,多人匹配的table {project} {table}'.format(project=self.project, table=table))
                elif len(match_table_df) == 1:
                    replace_table_name = '{table_catalog}.{table_schema}.{table_name}'.format(
                        table_catalog=match_table_df['table_catalog'].values[0],
                        table_schema=match_table_df['table_schema'].values[0],
                        table_name=match_table_df['table_name'].values[0])
                    self.sql = self.sql.replace(table, replace_table_name)
            else:
                print('table替换失败 {project} {table}'.format(project=self.project, table=table))
                # raise Exception('table替换失败 {project} {table}'.format(project=project, table=table))

    def replace_token0_token1(self):
        self.sql = self.sql.replace(' "amount0Out" ', ' CAST(amount0Out as FLOAT64) ')
        self.sql = self.sql.replace(' "amount1Out" ', ' CAST(amount1Out as FLOAT64) ')
        self.sql = self.sql.replace(' "amount0In" ', ' CAST(amount0In as FLOAT64) ')
        self.sql = self.sql.replace(' "amount1In" ', ' CAST(amount1In as FLOAT64) ')

    def process(self):
        self.extract_core()
        self.remove_type_define()
        self.replace_array()
        self.fix_usd_amount()
        self.remove_token()
        self.remove_price_table()
        self.replace_columns()
        self.remove_tx()
        self.replace_table()
        self.replace_token0_token1()
        return self.sql
