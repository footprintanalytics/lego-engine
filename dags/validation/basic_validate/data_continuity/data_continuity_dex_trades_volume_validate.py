from validation.basic_validate.data_continuity.data_continuity_validate import DataContinuityValidate


class DataContinuityDexTradesVolumeValidate(DataContinuityValidate):

    validate_name = 'data_continuity_dex_trades_volume_validate'
    desc = 'dex的trades业务的volume的连续性校验'
    validate_target = 'volume'
    switch_validate = True


    def get_self_sql(self):
        return """
        with dex_pair_daily as (
            select
            day,
            protocol_id,  
            sum(usd_amount) as volume
            from(
                select
                Date(k.block_time) as day,
                k.token_a_address,
                k.token_b_address,
                k.protocol_id,
                coalesce(SAFE_MULTIPLY(k.token_a_price, k.token_a_amount),SAFE_MULTIPLY(k.token_b_price, k.token_b_amount)) as usd_amount
                from (
                    select
                        t.block_time,
                        t.token_a_address,
                        t.token_b_address,
                        coalesce(t.token_a_amount,SAFE_DIVIDE(CAST(token_a_amount_raw AS FLOAT64), POW(10, erc_a.decimals))) as token_a_amount,
                        coalesce(t.token_b_amount,SAFE_DIVIDE(CAST(token_b_amount_raw AS FLOAT64), POW(10, erc_b.decimals))) as token_b_amount,
                        t.protocol_id,
                        token_a_price,
                        token_b_price
                        from(
                            select t0.*,
                            a.price,
                            a.price AS token_a_price,
                            b.price AS token_b_price,
                            info.chain
                            from (select * from `{table}` where Date(block_time)  {date})   t0 
                            left join `xed-project-237404.footprint_etl.defi_protocol_info` info
                            on info.protocol_id = t0.protocol_id
                            left join (select * from `footprint-etl-internal.view_to_table.token_daily_price` where Date(day) {date})a 
                            on lower(t0.token_a_address) = lower(a.token_address)          
                            and Date(t0.block_time) = Date(a.day) 
                            left join  (select * from `footprint-etl-internal.view_to_table.token_daily_price` where Date(day) {date}) b 
                            on lower(t0.token_b_address) = lower(b.token_address) 
                            and Date(t0.block_time) = Date(b.day) 
                        )  t
                    left join `xed-project-237404.footprint_etl.erc20_all`erc_a
                    on lower(t.token_a_address) = lower(erc_a.contract_address) and t.chain = erc_a.chain
                    left join `xed-project-237404.footprint_etl.erc20_all` erc_b
                    on lower(t.token_b_address) = lower(erc_b.contract_address) and t.chain=erc_b.chain
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
            from dex_pair_daily 
        )
        where date_diff(next_day,day,day) >1

        """.format(
            table=self.validate_table[0],
            date=self.date)