from inner_validate.base_model_validate import BaseModelValidate
from inner_validate.validate_cls import LessThanZeroValidate, DataNullValidate, LessThanOrEqZeroValidate


class LendingValidate(BaseModelValidate):
    lending_name: str

    repay_source_table = 'footprint-etl.footprint_lending.lending_repay'
    borrow_source_table = 'footprint-etl.footprint_lending.lending_borrow'
    supply_source_table = 'footprint-etl.footprint_lending.lending_supply'
    withdraw_source_table = 'footprint-etl.footprint_lending.lending_withdraw'

    def set_task_name(self, name, chain):
        self.task_name = '{}_{}_lending_model_validate'.format(chain, name)
        self.lending_name = name
        self.chain = chain

    def valid_deposit_info_less_than_zero(self):
        """
        存入余额小于0的校验
        """
        source_table = 'footprint-etl.footprint_lending.lending_collateral_change'

        sql = """
        select 
        sum(case when type in ('deposit','withdraw') then token_amount else 0 end) as deposit_balance,
        sum(case when type in ('deposit','liquidation') then token_amount else 0 end) as deposit_diff_liquidation,
        asset_address
        from (
            select c.*,d.chain from {collateral_change_table} c 
            left join `xed-project-237404.footprint_etl.defi_protocol_info` d
            on c.protocol_id = d.protocol_id
            where lower(c.project) = lower('{project}') and lower(d.chain) = lower('{chain}') and type in ('deposit','withdraw','liquidation')  
        )
        group by asset_address 
        """.format(collateral_change_table=source_table, project=self.lending_name, chain=self.chain)
        fields_info = [{'field': 'deposit_balance', 'value': 0}, {'field': 'deposit_diff_liquidation', 'value': 0}]

        valid_result = LessThanZeroValidate.validate_result(sql, fields_info)
        return {'source_table': [source_table], 'sql': sql, 'fields_info': fields_info, 'valid_result': valid_result,
                'rule_name': LessThanZeroValidate.valid_rule_name}

    def valid_outstanding_loan_less_than_zero(self):
        """
        贷款债务小于零的校验
        """
        borrow_source_table = 'footprint-etl.footprint_lending.lending_borrow'
        repay_source_table = 'footprint-etl.footprint_lending.lending_repay'

        sql = """
        select sum(token_amount) as outstanding_loan from (
        select * from (
             select asset_address, token_amount,protocol_id,project from {borrow_table}
             union all 
             select asset_address, -token_amount as token_amount,protocol_id,project from {repay_table}
            ) t
            left join `xed-project-237404.footprint_etl.defi_protocol_info` d
            on t.protocol_id = d.protocol_id
            where lower(t.project) = lower('{project}') and lower(d.chain) = lower('{chain}')
        ) k
        """.format(borrow_table=borrow_source_table, repay_table=repay_source_table, project=self.lending_name,
                   chain=self.chain)
        fields_info = [{'field': 'outstanding_loan', 'value': 0}]

        valid_result = LessThanZeroValidate.validate_result(sql, fields_info)
        return {'source_table': [borrow_source_table, repay_source_table], 'sql': sql, 'fields_info': fields_info,
                'valid_result': valid_result,
                'rule_name': LessThanZeroValidate.valid_rule_name}

    def valid_lending_asset_price(self):
        collateral_change_source_table = 'footprint-etl.footprint_lending.lending_collateral_change'
        repay_source_table = 'footprint-etl.footprint_lending.lending_repay'
        borrow_source_table = 'footprint-etl.footprint_lending.lending_borrow'
        supply_source_table = 'footprint-etl.footprint_lending.lending_supply'
        withdraw_source_table = 'footprint-etl.footprint_lending.lending_withdraw'

        sql = """
        with all_info as (
            select asset_address,day from (
            select * from (
                select asset_address,Date(block_timestamp) as day,protocol_id,project from {collateral_change_source_table}
                union all
                select asset_address,Date(block_timestamp) as day,protocol_id,project  from {repay_source_table}
                union all
                select asset_address,Date(block_timestamp) as day,protocol_id,project  from {borrow_source_table}
                union all
                select asset_address,Date(block_timestamp) as day,protocol_id,project  from {supply_source_table}
                union all
                select asset_address,Date(block_timestamp) as day,protocol_id,project  from {withdraw_source_table}
                ) t
            left join `xed-project-237404.footprint_etl.defi_protocol_info` d
            on t.protocol_id = d.protocol_id
            where lower(t.project) = lower('{project}') and lower(d.chain) = lower('{chain}')
            )
        )
        select p.address, price, p.day from  `xed-project-237404.footprint_etl.token_daily_price` p
        left join (
        select asset_address, day from all_info group by asset_address, day
        ) all_daily
        on p.address = all_daily.asset_address and all_daily.day = p.day     
        """.format(collateral_change_source_table=collateral_change_source_table,
                   repay_source_table=repay_source_table,
                   borrow_source_table=borrow_source_table,
                   supply_source_table=supply_source_table,
                   withdraw_source_table=withdraw_source_table,
                   project=self.lending_name,
                   chain=self.chain)
        fields_info = [{'field': 'price'}]
        valid_result = DataNullValidate.validate_result(sql, fields_info)
        return {'source_table': [borrow_source_table, repay_source_table, collateral_change_source_table,
                                 supply_source_table, withdraw_source_table], 'sql': sql,
                'fields_info': fields_info,
                'valid_result': valid_result,
                'rule_name': DataNullValidate.valid_rule_name}

    def valid_lending_asset_daily_trader_count(self):
        collateral_change_source_table = 'footprint-etl.footprint_lending.lending_collateral_change'
        repay_source_table = 'footprint-etl.footprint_lending.lending_repay'
        borrow_source_table = 'footprint-etl.footprint_lending.lending_borrow'
        supply_source_table = 'footprint-etl.footprint_lending.lending_supply'
        withdraw_source_table = 'footprint-etl.footprint_lending.lending_withdraw'

        sql = """
            with all_info as (
                select 
                    day,
                    type 
                from (
                select * from (
                        select Date(block_timestamp)as day,type,protocol_id,project from {collateral_change_source_table}
                        union all
                        select Date(block_timestamp)as day,'repay' as type,protocol_id,project  from {repay_source_table}
                        union all
                        select Date(block_timestamp)as day,'borrow' as type,protocol_id,project  from {borrow_source_table}
                        union all
                        select Date(block_timestamp)as day,'deposit' as type,protocol_id,project   from {supply_source_table}
                        union all
                        select Date(block_timestamp)as day,'withdraw' as type,protocol_id,project  from {withdraw_source_table}
                        ) t
                        left join `xed-project-237404.footprint_etl.defi_protocol_info` d
                         on t.protocol_id = d.protocol_id
                        where lower(t.project) = lower('{project}') and lower(d.chain) = lower('{chain}')
                    ) 
                    group by day,type
            )
            select 
            case when type='deposit' then deposit_trader_count else 0 end as deposit_trader_count,
            case when type='withdraw' then withdraw_trader_count else 0 end as withdraw_trader_count,
            case when type='repay' then repay_trader_count else 0 end as repay_trader_count,
            case when type='borrow' then borrow_trader_count else 0 end as borrow_trader_count,
            case when type='liquidation' then liquidation_trader_count else 0 end as liquidation_trader_count
            from all_info a 
            left join `xed-project-237404.footprint.lending_assets_daily_stats` l
            on a.day = l.day
            """.format(collateral_change_source_table=collateral_change_source_table,
                       repay_source_table=repay_source_table,
                       borrow_source_table=borrow_source_table, supply_source_table=supply_source_table,
                       withdraw_source_table=withdraw_source_table, project=self.lending_name, chain=self.chain)

        fields_info = [{'field': 'deposit_trader_count'}, {'field': 'withdraw_trader_count'},
                       {'field': 'repay_trader_count'}, {'field': 'borrow_trader_count'},
                       {'field': 'liquidation_trader_count'}]
        valid_result = DataNullValidate.validate_result(sql, fields_info)
        return {'source_table': [borrow_source_table, repay_source_table, collateral_change_source_table,
                                 supply_source_table, withdraw_source_table],
                'sql': sql,
                'fields_info': fields_info,
                'valid_result': valid_result,
                'rule_name': DataNullValidate.valid_rule_name}

    def valid_token_amount_less_than_zero(self):
        sql = """
        SELECT
          DISTINCT(asset_address) as asset_address, source_table, token_amount
        FROM (
          SELECT asset_address, 'lending_borrow' as source_table, token_amount FROM `footprint-etl.footprint_lending.lending_borrow` WHERE lower(project) = lower('{project}') AND token_amount < 0
          UNION ALL
          SELECT asset_address, 'lending_repay' as source_table, token_amount FROM `footprint-etl.footprint_lending.lending_repay` WHERE lower(project) = lower('{project}') AND token_amount < 0
          UNION ALL
          SELECT asset_address, 'lending_supply' as source_table, token_amount FROM `footprint-etl.footprint_lending.lending_supply` WHERE lower(project) = lower('{project}') AND token_amount < 0
          UNION ALL
          SELECT asset_address, 'lending_withdraw' as source_table, token_amount FROM `footprint-etl.footprint_lending.lending_withdraw` WHERE lower(project) = lower('{project}') AND token_amount < 0
          )
        LIMIT 5
        """.format(project=self.lending_name)
        fields_info = [{'field': 'token_amount'}]
        valid_result = LessThanZeroValidate.validate_result(sql, fields_info)
        return {'source_table': [self.repay_source_table,
                                 self.borrow_source_table,
                                 self.supply_source_table,
                                 self.withdraw_source_table],
                'sql': sql,
                'fields_info': fields_info,
                'valid_result': valid_result,
                'rule_name': LessThanZeroValidate.valid_rule_name
                }

    def valid_net_assets_less_than_zero(self):
        sql = """
        SELECT
          asset_address, sum(token_amount) as net_assets
        FROM (
          SELECT asset_address, -token_amount AS token_amount, block_timestamp FROM `footprint-etl.footprint_lending.lending_borrow` WHERE lower(project) = lower('{project}') 
          UNION ALL
          SELECT asset_address, token_amount AS token_amount, block_timestamp FROM `footprint-etl.footprint_lending.lending_repay` WHERE lower(project) = lower('{project}') 
          UNION ALL
          SELECT asset_address, token_amount AS token_amount, block_timestamp FROM `footprint-etl.footprint_lending.lending_supply` WHERE lower(project) = lower('{project}') 
          UNION ALL
          SELECT asset_address, -token_amount AS token_amount, block_timestamp FROM `footprint-etl.footprint_lending.lending_withdraw` WHERE lower(project) = lower('{project}') 
          )
        GROUP BY
          asset_address
        HAVING
          SUM(token_amount) < 0
        LIMIT 5
        """.format(project=self.lending_name)
        fields_info = [{'field': 'net_assets'}]
        valid_result = LessThanZeroValidate.validate_result(sql, fields_info)
        return {'source_table': [self.repay_source_table,
                                 self.borrow_source_table,
                                 self.supply_source_table,
                                 self.withdraw_source_table],
                'sql': sql,
                'fields_info': fields_info,
                'valid_result': valid_result,
                'rule_name': LessThanZeroValidate.valid_rule_name
                }

    def get_valid_steps(self):
        return [
            self.valid_net_assets_less_than_zero,
            self.valid_token_amount_less_than_zero
        ]
