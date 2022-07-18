SELECT
        'Harvest Finance' as project,
        '1' as version,
        8 as protocol_id,
        'deposit' as type,
        s.block_number,
        s.block_timestamp,
        s.transaction_hash,
        s.log_index,
        contract_address,
        operator,
        asset_address,
        CAST(amount as float64) as asset_amount,
        concat(contract_address,'_8') as pool_id
        from (
        -- 其他币与Farm的pool
            select vd.amount,vd.transaction_hash,vd.contract_address,vd.log_index,vd.block_number,vd.block_timestamp,if(hd.origin is null,operator,hd.origin) operator ,t.token_address as asset_address
            from (
            select amount,transaction_hash,contract_address,log_index,block_number,block_timestamp,beneficiary as operator from `footprint-etl.ethereum_harvest.Vault_event_Deposit` where Date(block_timestamp) {match_date_filter}
            union all
            select amount,transaction_hash,contract_address,log_index,block_number,block_timestamp,user as operator from `footprint-etl.ethereum_harvest.NoMintRewardPool_event_Staked` where Date(block_timestamp) {match_date_filter}
            )vd
--             left join (select pool_id,stake_token[safe_offset(0)] as token,name from `footprint-etl.footprint_pool_infos.pool_infos` where protocol_id=8) p on p.pool_id=vd.contract_address
            left join (select * from `footprint-blockchain-etl.crypto_ethereum.token_transfers` where Date(block_timestamp) {match_date_filter})t
            on vd.transaction_hash=t.transaction_hash and vd.contract_address=t.to_address  and vd.amount=t.value
            left join(select * from `footprint-etl.ethereum_harvest.HarvestSC_event_Deposit` where Date(block_timestamp) {match_date_filter})  hd
            on vd.transaction_hash=hd.transaction_hash and vd.operator=hd.contract_address
            union all
        -- iFarm的pool
            select balanceOf as amount,transaction_hash,contract_address,log_index,block_number,block_timestamp,user operator,'0xa0246c9032bc3a600820415ae600c6388619a14d' asset_address
            from `footprint-etl.ethereum_harvest.AutoStake_event_Staked`  where Date(block_timestamp) {match_date_filter} and user!='0xf2004f64f71f110e9e50b5ff36253fe8785b2bcc'
        ) s 