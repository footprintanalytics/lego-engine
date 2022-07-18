from utils import Constant
from utils.monitor import save_monitor
from datetime import datetime,timedelta
import pydash
from utils.date_util import DateUtil
from utils.print_util import print_red
from inner_validate.validate_cls import AddressFormatValid


class BaseModelValidate:
    valid_model_instance: str
    task_name: str
    date_str: str
    project_id: str = 'footprint-etl'

    def __init__(self, name: str = None, chain=None, execution_date: datetime = None, save_to_db: bool = True):
        self.name = name
        if not execution_date:
            execution_date = self.get_execution_date()
        self.execution_date = execution_date
        self.execution_date_str = self.execution_date.strftime('%Y-%m-%d')
        self.save_to_db = save_to_db
        self.set_task_name(name,chain)

    def set_task_name(self, name,chain):
        self.task_name = name
        self.chain = chain

    def get_execution_date(self):
        return DateUtil.utc_start_of_date()

    def get_valid_steps(self):
        """
        需要内部校验的步骤
        """
        return []

    def get_rule_info(self, valid_rule_name):
        """
        获取相关校验规则的信息
        """
        rule_name = Constant.DASH_BOARD_RULE_NAME[valid_rule_name]
        desc = '{task_name} rule_name is {rule_name}'.format(task_name=self.task_name, rule_name=rule_name)
        desc_cn = '{}_{}'.format(Constant.DASH_BOARD_RULE_NAME_DESC_CN[valid_rule_name])
        return rule_name, desc, desc_cn

    def handle_valid_file_result(self, info, valid_rule_name, valid_result):
        """
        处理校验结果
        """
        rule_name = Constant.DASH_BOARD_RULE_NAME[valid_rule_name]
        desc = '{task_name} rule_name is {rule_name}'.format(task_name=self.task_name, rule_name=rule_name)
        desc_cn = '{}'.format(Constant.DASH_BOARD_RULE_NAME_DESC_CN[valid_rule_name])
        match_field = list((filter(lambda n: n.get('field') == info.get('field'), valid_result)))
        result_code = Constant.DASH_BOARD_RESULT_CODE['REGULAR']
        count = 0
        if len(match_field)> 0:
            print_red(f'{self.task_name}  valid fail field {info.get("field")} {rule_name}' )
            result_code = Constant.DASH_BOARD_RESULT_CODE['EXCEPTION']
            count = match_field[0].get('num')
        return rule_name, desc, desc_cn, result_code, count

    def valid_address_format(self):
        """
        address小于42位或不是0x开头的校验
        """
        borrow_source_table = 'footprint-etl.footprint_lending.lending_borrow'
        repay_source_table = 'footprint-etl.footprint_lending.lending_repay'
        supply_source_table = 'footprint-etl.footprint_lending.lending_supply'
        withdraw_source_table = 'footprint-etl.footprint_lending.lending_withdraw'
        add_liquidity_source_table = 'footprint-etl.footprint_dex.dex_add_liquidity'
        remove_liquidity_source_table = 'footprint-etl.footprint_dex.dex_remove_liquidity'
        trades_source_table = 'footprint-etl.footprint_dex.dex_trades'

        sql = """
        select 
        project,
        op_user,
        token_address,
        contract_address,
        from (
            select * from (
                select protocol_id,asset_address as token_address,contract_address,borrower as op_user,project from {borrow_source_table} 
                union all
                select protocol_id,asset_address as token_address,contract_address,borrower as op_user,project from {repay_source_table} 
                union all
                select protocol_id,asset_address as token_address,contract_address,borrower as op_user,project from {supply_source_table} 
                union all
                select protocol_id,asset_address as token_address,contract_address,borrower as op_user,project from {withdraw_source_table} 
                union all
                select protocol_id,token_address ,exchange_address as contract_address,liquidity_provider as op_user,project from {add_liquidity_source_table} 
                union all
                select protocol_id,token_address ,exchange_address as contract_address,liquidity_provider as op_user,project from {remove_liquidity_source_table} 
                union all
                select protocol_id,token_a_address as token_address,exchange_contract_address as contract_address,trader_a as op_user,project from {trades_source_table} 
                union all
                select protocol_id,token_b_address as token_address,exchange_contract_address as contract_address,trader_b as op_user,project from {trades_source_table}
                ) c
                left join `xed-project-237404.footprint_etl.defi_protocol_info` d
                on c.protocol_id = d.protocol_id
                where lower(c.project) = lower('{project}') and lower(d.chain) = lower('{chain}')
        )
        """.format(borrow_source_table=borrow_source_table, repay_source_table=repay_source_table, supply_source_table=supply_source_table, withdraw_source_table=withdraw_source_table, add_liquidity_source_table=add_liquidity_source_table, remove_liquidity_source_table=remove_liquidity_source_table, trades_source_table=trades_source_table, project=self.name, chain=self.chain)
        fields_info = [{'field': 'token_address', 'value': ''}, {'field': 'contract_address', 'value': ''}, {'field': 'op_user', 'value': ''}]

        valid_result = AddressFormatValid.validate_result(sql, fields_info)
        return {'source_table': [borrow_source_table, repay_source_table, supply_source_table, withdraw_source_table, add_liquidity_source_table, remove_liquidity_source_table, trades_source_table], 'sql': sql, 'fields_info': fields_info, 'valid_result': valid_result,
                'rule_name': AddressFormatValid.valid_rule_name}

    def valid_steps(self):
        base_valid_steps = [
            self.valid_address_format
        ]
        return base_valid_steps + self.get_valid_steps()

    def validate(self):
        """
        内部校验
        """
        valid_func = self.valid_steps()
        print(valid_func)
        result_list = []
        for func in valid_func:
            info = func()
            source_table, sql, fields_info, valid_result, valid_rule_name, value = pydash.get(info, 'source_table'),\
                pydash.get(info,'sql'),\
                pydash.get(info, 'fields_info'),\
                pydash.get(info, 'valid_result'),\
                pydash.get(info, 'rule_name'),\
                pydash.get(info, 'value')

            for info in fields_info:
                rule_name, desc, desc_cn, result_code, count = self.handle_valid_file_result(info, valid_rule_name, valid_result)
                result_list.append({
                    'result_code': result_code,
                    'rule_name': rule_name,
                    'desc_cn': desc_cn,
                    'desc': desc,
                    'count': count,
                    'field': info.get('field')
                })

            if not fields_info:
                result_list.append({
                    'result_code': Constant.DASH_BOARD_RESULT_CODE['REGULAR'] if valid_result else Constant.DASH_BOARD_RESULT_CODE['EXCEPTION'],
                    'rule_name': valid_rule_name,
                    'desc_cn': '',
                    'desc': '',
                    'count': value,
                    'field': info.get('field')
                })

            if self.save_to_db and result_list:
                for result in result_list:
                    save_monitor(
                         task_name=self.task_name,
                         execution_date=datetime.strptime(self.execution_date_str, '%Y-%m-%d'),
                         bigquery_etl_database=self.project_id, table_name=','.join(source_table),
                         rule_name=result.get('rule_name'),
                         item_value=result.get('count'),
                         result_code=result.get('result_code'),
                         desc=result.get('desc'),
                         desc_cn=result.get('desc_cn'),
                         sql=sql,
                         field=info.get('field')
                    )

        return result_list

    def airflow_dag_params(self):
        dag_params = {
            "dag_id": "footprint_validate_{}_dag".format(self.task_name),
            "catchup": False,
            "schedule_interval": '0 2 * * *',
            "description": "{}_dag".format(self.task_name),
            "default_args": {
                'owner': 'airflow',
                'depends_on_past': False,
                'retries': 1,
                'retry_delay': timedelta(minutes=5),
                'start_date': datetime(2021, 8, 20)
            },
            "dagrun_timeout": timedelta(days=30)
        }
        print('dag_params', dag_params)
        return dag_params


