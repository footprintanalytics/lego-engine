from ..abnormal_number_validate import AbnormalNumberValidate


class DexSwapAbnormalNumberValidate(AbnormalNumberValidate):

    def get_self_sql(self):
        sql = """
            select
                count(1) as count,
                ARRAY_AGG(distinct token_a_symbol IGNORE NULLS) as a_symbol,
                ARRAY_AGG(distinct token_b_symbol IGNORE NULLS) as b_symbol,
                ARRAY_AGG(distinct token_a_address IGNORE NULLS) as a_address,
                ARRAY_AGG(distinct token_b_address IGNORE NULLS) as b_address
            from
            (SELECT
              SAFE_MULTIPLY(cast (token_a_amount as float64), a_price) as token_a_value,
              SAFE_MULTIPLY(cast (token_b_amount as float64), b_price) as token_b_value,
              *
            FROM
              `{table}` t
            left join (
                select token_address as a_addrss, max(price) as a_price, Date(day) as time from `footprint-etl-internal.view_to_table.token_daily_price`
                group by token_address, Date(day)
            ) p_a
            on (t.token_a_address = p_a.a_addrss and Date(t.block_time) = p_a.time)
            
            left join (
                select token_address as b_address, max(price) as b_price, Date(day) as time from `footprint-etl-internal.view_to_table.token_daily_price`
                group by token_address, Date(day)
            ) p_b
            on (t.token_b_address = p_b.b_address and Date(t.block_time) = p_b.time)
            )
            where token_a_value > {max} or token_b_value > {max}
        """.format(table=self.validate_table[0], max=self.validate_args["max"])

        return sql
