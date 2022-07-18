from inner_validate.base_model_validate import BaseModelValidate
from inner_validate.validate_cls import LessThanZeroValidate, AnomalyValidate, PoolBalanceValidate
from datetime import datetime
import moment
import pydash
from utils.date_util import DateUtil


class DexValidate(BaseModelValidate):
    dex_name: str

    def set_task_name(self, name, chain):
        self.task_name = '{}_{}_dex_model_validate'.format(chain,name)
        self.dex_name = name
        self.chain = chain

    def valid_token_balance_less_than_zero(self):
        add_liquidity_source_table = 'footprint-etl.footprint_dex.dex_add_liquidity'
        remove_liquidity_source_table = 'footprint-etl.footprint_dex.dex_remove_liquidity'
        trades_source_table = 'footprint-etl.footprint_dex.dex_trades'

        sql = """
        with swap as (
            select 
                sum (token_a_amount) as amount,
                token_a_address as token_address,
                exchange_contract_address as exchange_address
            from ( 
                select t.*,d.chain
                from {trades_source_table} t
                left join `xed-project-237404.footprint_etl.defi_protocol_info` d
                on t.protocol_id = d.protocol_id
                where lower(t.project) = '{project}' and lower(d.chain) = lower('{chain}')
            )
            group by token_a_address,exchange_contract_address
            
            union all
            
            select 
                sum (-token_b_amount) as amount,
                token_b_address as token_address,
                exchange_contract_address as exchange_address
            from  ( 
                select t.*,d.chain
                from {trades_source_table} t
                left join `xed-project-237404.footprint_etl.defi_protocol_info` d
                on t.protocol_id = d.protocol_id
                where lower(t.project) = '{project}' and lower(d.chain) = lower('{chain}')
            )
            group by token_b_address,exchange_contract_address         
        ), all_info as (
            select 
                sum(token_amount) as amount,
                token_address,
                exchange_address 
            from (
                select t.*,d.chain from (
                    select * from {add_liquidity_source_table}
                    union all
                    select * from {remove_liquidity_source_table}
                ) t
                left join `xed-project-237404.footprint_etl.defi_protocol_info` d
                on t.protocol_id = d.protocol_id
                where lower(t.project) = '{project}' and lower(d.chain) = lower('{chain}')
            )             
            group by token_address,exchange_address
            
            union all 
            
            select 
                sum(-amount) as amount,
                token_address,
                exchange_address 
            from swap 
            group by token_address,exchange_address
        )
        
        select 
        sum(amount) as token_balance,
        token_address,
        exchange_address  
        from all_info 
        group by token_address,exchange_address
        """.format(
            trades_source_table=trades_source_table,
            add_liquidity_source_table=add_liquidity_source_table,
            remove_liquidity_source_table=remove_liquidity_source_table,
            project=self.dex_name, chain=self.chain
        )

        fields_info = [{'field': 'token_balance', 'value': 0}]

        valid_result = LessThanZeroValidate.validate_result(sql, fields_info)
        return {
            'source_table': [add_liquidity_source_table, remove_liquidity_source_table, trades_source_table],
            'sql': sql,
            'fields_info': fields_info,
            'valid_result': valid_result,
            'rule_name': LessThanZeroValidate.valid_rule_name
        }

    def valid_volume_anomaly(self):
        date15ago = "'" + datetime.strftime(DateUtil.utc_x_hours_ago(24 * 15, self.execution_date), '%Y-%m-%d') + "'"
        trades_source_table = 'footprint-etl.footprint_dex.dex_trades'
        sql = """
        with swap as (
            select 
                sum (token_a_amount) as amount,
                token_a_address as token_address,
                exchange_contract_address as exchange_address,
                Date(block_time) as day
            from ( 
                select t.*,d.chain
                from {trades_source_table} t
                left join `xed-project-237404.footprint_etl.defi_protocol_info` d
                on t.protocol_id = d.protocol_id
                where lower(t.project) = '{project}' and lower(d.chain) = lower('{chain}') and Date(block_time)>{dateAgo}
            )
            group by token_a_address,exchange_contract_address,Date(block_time)
            
            union all
            
            select 
                sum (token_b_amount) as amount,
                token_b_address as token_address,
                exchange_contract_address as exchange_address,
                Date(block_time) as day
            from  ( 
                select t.*,d.chain
                from {trades_source_table} t
                left join `xed-project-237404.footprint_etl.defi_protocol_info` d
                on t.protocol_id = d.protocol_id
                where lower(t.project) = '{project}' and lower(d.chain) = lower('{chain}') and Date(block_time)>{dateAgo}
            )
            group by token_b_address,exchange_contract_address,Date(block_time)         
        ),
         daily AS 
            (SELECT day,
                  CONCAT(token_address,
                         "_",
                        exchange_address) AS id,
                 token_address,
                 exchange_address,
                 SUM(amount) AS token_amount
            FROM swap
            GROUP BY  day, token_address, exchange_address ), avgfor3d AS 
            (SELECT day,
                id,
                token_amount,
                (SELECT (sum(token_amount) / 3)
                FROM daily d2
                WHERE d2.day < d1.day
                        AND d2.day >= DATE_SUB(d1.day, INTERVAL 3 DAY)
                        AND d2.id = d1.id ) AS token_amount_past_3d
                FROM daily d1 )
            SELECT 
            case when d.token_amount > d.token_amount_past_3d * 50 or  d.token_amount < d.token_amount_past_3d * (1/50) then 1 else 0 end as anomaly_tag
            FROM avgfor3d d
        """.format(dateAgo=date15ago, trades_source_table=trades_source_table, project=self.dex_name,chain=self.chain)

        fields_info = [{'field': 'anomaly_tag'}]
        valid_result = AnomalyValidate.validate_result(sql, fields_info)
        return {
            'source_table': [trades_source_table],
            'sql': sql,
            'fields_info': fields_info,
            'valid_result': valid_result,
            'rule_name': AnomalyValidate.valid_rule_name
        }

    def valid_pool_token_balance(self):
        check_day_month_ago = moment.now().add(days=-30).format('YYYY-MM-DD')
        dex_check_liquidity_sql = """
            select count(1) as count, project, result
            from (
                select
                dex_pool_change.project as project,
                dex_pool_change.exchange_address as exchange_address,
                dex_pool_change.token_address as token_address,
                dex_pool_change.date as date,
                dex_pool_change.amount as dex_amount,
                pool_token_balance.amount_raw as pool_token_amount,
                (dex_pool_change.amount - pool_token_balance.amount_raw) as diff,
                if(abs(pool_token_balance.amount_raw * 0.1) > abs((dex_pool_change.amount - pool_token_balance.amount_raw)), true, false) as result
                from (
                    select exchange_address, token_address, sum(token_amount_raw) as amount, Date(block_time) as date, max(project) as project from (
                    select exchange_address, project, token_address, token_amount_raw, block_time FROM `footprint-etl.footprint_dex.dex_add_liquidity` 
                    
                    union all
                    
                    SELECT exchange_address, project, token_address, -token_amount_raw as token_amount_raw, block_time FROM `footprint-etl.footprint_dex.dex_remove_liquidity`
                    
                    union all
                    
                    -- pool 减少 token_a 流动性
                    select
                    exchange_contract_address as exchange_address,
                    project,
                    token_a_address as token_address,
                    -token_a_amount_raw as token_amount_raw,
                    block_time
                    from `footprint-etl.footprint_dex.dex_trades`
                    where lower(chain) = '{chain}'
                
                    union all
                
                    -- pool 增加 token_b 流动性
                    select
                    exchange_contract_address as exchange_address,
                    project,
                    token_b_address as token_address,
                    token_b_amount_raw as token_amount_raw,
                    block_time
                    from `footprint-etl.footprint_dex.dex_trades`
                    where lower(chain) = '{chain}'
                ) dex
                where lower(dex.project) = '{project}'
                and Date(dex.block_time) > '{check_day_month_ago}'
                and Date(dex.block_time) < '{check_day}'
                group by exchange_address, token_address, Date(block_time)
                ) dex_pool_change
                left join
                (
                    SELECT * FROM `xed-project-237404.footprint_model_etl.ethereum_token_balance_change_day` WHERE DATE(timestamp) > '{check_day_month_ago}'
                    and Date(timestamp) < '{check_day}'
                    ORDER BY timestamp desc
                ) pool_token_balance
                on dex_pool_change.exchange_address = pool_token_balance.address
                and dex_pool_change.token_address = pool_token_balance.token_address
                and dex_pool_change.date = Date(pool_token_balance.timestamp)
                order by date desc, exchange_address asc
            )
            where lower(project) = '{project}'
            group by project, result
        """.format(
            check_day='2021-11-08',
            project=self.dex_name,
            check_day_month_ago=check_day_month_ago,
            chain=self.chain
        )

        valid_result = PoolBalanceValidate.validate_result(dex_check_liquidity_sql)
        print(valid_result)
        return {
            'source_table': ['footprint-etl.footprint_dex.dex_trades', 'footprint-etl.footprint_dex.dex_trades'],
            'sql': dex_check_liquidity_sql,
            'fields_info': [],
            'valid_result': pydash.get(valid_result, 'result'),
            'value': pydash.get(valid_result, 'value'),
            'rule_name': PoolBalanceValidate.valid_rule_name
        }

    def get_valid_steps(self):
        return [
            self.valid_token_balance_less_than_zero,
            self.valid_volume_anomaly,
            self.valid_pool_token_balance
        ]


if __name__ == '__main__':
    validate = DexValidate()
    validate.valid_pool_token_balance()
