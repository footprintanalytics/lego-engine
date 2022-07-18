from validation.basic_validate.data_continuity.data_continuity_validate import DataContinuityValidate


class DataContinuityLendingVolumeValidateLiquidation(DataContinuityValidate):
    validate_name = 'data_continuity_lending_volume_validate'
    desc = 'lending业务的volume的连续性校验'
    validate_target = 'volume'
    slack_warn = False

    def get_self_sql(self):
        return """
        with lending_daily as (
            select
            day,
            protocol_id,  
            sum(repay_usd_amount) as repay_volume,
            sum(collateral_usd_amount) as collateral_volume
            from(
                select
                Date(k.block_time) as day,
                k.token_address as token_address,
                k.protocol_id,
                SAFE_MULTIPLY(k.repay_token_price, k.repay_token_amount) as repay_usd_amount,
                SAFE_MULTIPLY(k.token_collateral_price, k.collateral_token_amount) as collateral_usd_amount,
                from (
                    select
                        t.block_time,
                        t.repay_token_address as token_address,
                        t.protocol_id,
                        token_collateral_price,
                        repay_token_price,
                        t.repay_token_amount as repay_token_amount,
                        t.token_collateral_amount as collateral_token_amount
                        from(
                            select t0.*,
                            a.price,
                            a.price AS token_collateral_price,
                            b.price,
                            a.price AS repay_token_price
                            from (select * from {table} where Date(block_time) {date})   t0 
                            left join (select * from `footprint-etl-internal.view_to_table.token_daily_price` where Date(day) {date})a 
                            on lower(t0.repay_token_address) = lower(a.token_address)
                            and Date(t0.block_time) = Date(a.day) 
                            left join (select * from `footprint-etl-internal.view_to_table.token_daily_price` where Date(day) {date})b
                            on lower(t0.token_collateral_address)= lower(b.token_address)   
                            and Date(t0.block_time) = Date(b.day) 
                        )  t
                    )k
                ) group by 1,2
                having repay_volume is not null and collateral_volume is not null
        )
        select
        *,
        concat(date_add(day,interval 1 day),' ~ ', date_add(next_day,interval -1 day)) as loss_date_data
        from ( 
            select
            day,
            lead(day, 1, Date(current_timestamp())) OVER (PARTITION BY protocol_id ORDER BY day) AS next_day,
            protocol_id
            from lending_daily 
        )
        where date_diff(next_day,day,day) > {interval_days}

        """.format(table=self.validate_table[0], date=self.date,interval_days = self.interval_days)