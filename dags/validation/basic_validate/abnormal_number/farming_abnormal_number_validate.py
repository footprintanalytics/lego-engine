from ..abnormal_number_validate import AbnormalNumberValidate
import re
import pydash
from utils.query_bigquery import query_bigquery


class FarmingAbnormalNumberValidate(AbnormalNumberValidate):
    switch_validate = True

    def get_self_sql(self):
        sql = """
            select
                count(1) as count,
                ARRAY_AGG(distinct token_symbol IGNORE NULLS) as symbol,
                ARRAY_AGG(distinct token_address IGNORE NULLS) as address
            from
            (SELECT
              SAFE_MULTIPLY(cast (token_amount as float64), price) as token_value,
              *
            FROM
              `{table}` t
            left join (
                select token_address as address, max(price) as price, Date(day) as time from `footprint-etl-internal.view_to_table.token_daily_price`
                group by token_address, Date(day)
            ) p
            on (t.token_address = p.address and Date(t.block_time) = p.time)
            )
            where token_value > {max}
        """.format(table=self.validate_table[0], max=self.validate_args["max"])

        return sql

