from validation.basic_validate.data_continuity.data_continuity_validate import DataContinuityValidate


class DataContinuityDexLiquidityVolumeValidate(DataContinuityValidate):
    validate_name = 'data_continuity_dex_liquidity_volume_validate'
    desc = 'dex liquidity业务的volume的连续性校验'
    validate_target = 'volume'

    def get_self_sql(self):
        res = """
        with dex_liquidity_daily as (
            select
            day,
            protocol_id,  
            sum(usd_amount) as volume
            from(
                select
                Date(k.block_time) as day,
                k.token_address,
                k.protocol_id,
                SAFE_MULTIPLY(k.token_price, k.token_amount) as usd_amount
                from (
                    select
                        t.block_time,
                        t.token_address,
                        t.protocol_id,
                        token_price,
                        token_amount,
                        from(
                            select t0.*,
                            a.price,
                            a.price AS token_price,
                            from (select * from {table} where Date(block_time) {date})  t0 
                            left join (select * from `footprint-etl-internal.view_to_table.token_daily_price` where Date(day) {date})a 
                            on lower(t0.token_address) = lower(a.token_address)          
                            and Date(t0.block_time)= Date(a.day) 
                        )  t
                    )k
                ) group by 1,2
                having volume is not null
        )
        select
        *,
        concat(date_add(day,interval 1 day),' ~ ', date_add(next_day,interval -1 day)) as loss_date_data
        from ( 
            select
            day,
            lead(day, 1, Date(current_timestamp())) OVER (PARTITION BY protocol_id ORDER BY day) AS next_day,
            protocol_id
            from dex_liquidity_daily 
        )
        where date_diff(next_day,day,day) >1

        """.format(table=self.validate_table, date=self.date)
        print(res)
        return res
