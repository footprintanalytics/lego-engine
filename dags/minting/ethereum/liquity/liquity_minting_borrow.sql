with total as (
        select * from `footprint-etl-internal.ethereum_liquity.BorrowerOperations_event_TroveUpdated`
        union all
        select * from `footprint-etl-internal.ethereum_liquity.TroveManager_event_TroveUpdated`
),
today_log as (
        select _borrower, transaction_hash, log_index, 1 as filter from `footprint-etl-internal.ethereum_liquity.BorrowerOperations_event_TroveUpdated` WHERE Date(block_timestamp) {match_date_filter}
        union all
        select _borrower, transaction_hash, log_index, 1 as filter from `footprint-etl-internal.ethereum_liquity.TroveManager_event_TroveUpdated` WHERE Date(block_timestamp) {match_date_filter}
),
today_borrower as (
    select distinct _borrower as borrower from today_log
),
today_update_user as (
    select total.* from total join today_borrower on today_borrower.borrower=total._borrower where total._borrower is not null
),
update_status as (
    select
        lag(_debt) over (partition by _borrower order by block_number, log_index) as before_debt,
        last_value(_debt) over (partition by _borrower order by block_number, log_index) as now_debt,
        lag(_coll) over (partition by _borrower order by block_number, log_index) as before_coll,
        last_value(_coll) over (partition by _borrower order by block_number, log_index) as now_coll,
        *
    from today_update_user
),
compare_table as (
    select
        cast (ifnull(before_debt, '0') as BIGNUMERIC) as before_debt,
        cast (now_debt as BIGNUMERIC) as now_debt,
        cast (ifnull(before_coll, '0') as BIGNUMERIC) as before_coll,
        cast (now_coll as BIGNUMERIC) as now_coll,
        update_status.* except (before_debt, before_coll, now_debt, now_coll)
    from
        update_status
    left join
        today_log on update_status.transaction_hash=today_log.transaction_hash and update_status.log_index=today_log.log_index and today_log._borrower=update_status._borrower

    where today_log.filter is not null -- and liquidation.block_timestamp is null
),
no_liquidation as (
    select compare_table.* from compare_table left join
            (select distinct transaction_hash as transaction_hash, 1 as filter from `footprint-etl-internal.ethereum_liquity.TroveManager_event_TroveLiquidated`) as liquidation
    on compare_table.transaction_hash=liquidation.transaction_hash where liquidation.filter is null
)
SELECT
	'Liquity' as project,
	'1' as version,
	183 as protocol_id,
	'minting' as type,
	block_number,
	block_timestamp as block_time,
	transaction_hash as tx_hash,
	log_index,
	contract_address,
	_borrower as operator,
	'0x5f98805a4e8be255a32880fdec7f6728c6568ba0' as token_address,
	now_debt - before_debt as token_amount_raw,
     '' as pool_id
FROM
	no_liquidation
WHERE
    before_debt < now_debt