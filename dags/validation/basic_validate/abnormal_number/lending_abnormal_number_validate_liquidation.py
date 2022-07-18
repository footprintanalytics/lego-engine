from ..abnormal_number_validate import AbnormalNumberValidate


class LendingAbnormalNumberValidateLiquidation(AbnormalNumberValidate):

    def get_self_sql(self):
        sql = """
            select
                count(1) as count,
                ARRAY_AGG(distinct token_symbol IGNORE NULLS) as symbol,
                ARRAY_AGG(distinct token_address IGNORE NULLS) as address
            from
            (SELECT
              SAFE_MULTIPLY(cast (token_amount as float64), price) as token_collateral_value,
              *
            FROM
              (select tx_hash,log_index,contract_address,
              block_time,
              token_collateral_symbol as token_symbol,
              token_collateral_address as token_address,
              token_collateral_amount as token_amount ,
              "collateral" as type
              from `{table}`
              union all 
              select tx_hash,log_index,contract_address,
              block_time,
              repay_token_symbol as token_symbol,
              repay_token_address as token_address,
              repay_token_amount as token_amount ,
              "repay" as type
              from `{table}`
              ) t
            left join (
                select token_address, max(price) as price, Date(day) as time from `footprint-etl-internal.view_to_table.token_daily_price`
                group by token_address, Date(day)
            ) p
            on (t.token_address = p.address and Date(t.block_time) = p.time)
            )
            where token_collateral_value > {max}
        """.format(table=self.validate_table[0], max=self.validate_args["max"])

        return sql
