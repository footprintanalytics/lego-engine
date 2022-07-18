with holders as (
    select token_id,pool_address,day from `gaia-dao.gaia_dao.gaia__pool_holders`  h where pool_address in('0x8981c60ff02cdbbf2a6ac1a9f150814f9cf68f62', '0x01cc44fc1246d17681b325926865cdb6242277a5')
),
positions as (
    select * from `gaia-dao.gaia_dao.univ3_nft_positions_daily` order by  date
),
positions_holders as (
    select positions.*,holders.pool_address from holders left join positions on holders.token_id = positions.tokenId and holders.day = positions.date

),
tvl as (
    select pool_address,date,tokenId,chain, tickLower, tickUpper, liquidity, `footprint-etl.Ethereum_izumi_autoparse.get_percent_event_function`(tickLower, tickUpper) as percent, `footprint-etl.Ethereum_izumi_autoparse.get_percent_event_function`(tickLower, tickUpper) * POW(10, -9) * liquidity as valid_liquidity  from positions_holders
)
select sum(valid_liquidity) true_tvl,pool_address,date day,chain from tvl where  date is not null group by pool_address,date,chain order by date