SELECT
                'Harvest Finance' as project,
                '1' as version,
                8 as protocol_id,
                'reward' as type,
                block_number,
                block_timestamp,
                transaction_hash,
                log_index,
                contract_address,
                operator,
                asset_address,
                CAST(reward as float64) as asset_amount,
                concat(contract_address,'_8') as pool_id
                from (
                --nomintpool
                    select
                    block_number,
                    block_timestamp,
                    transaction_hash,
                    log_index,
                    contract_address,
                    user as operator,'0xa0246c9032bc3a600820415ae600c6388619a14d' as asset_address,reward
                    from `footprint-etl.ethereum_harvest.NoMintRewardPool_event_RewardPaid`

                    union all
                    --potpool
                    select
                    block_number,
                    block_timestamp,
                    transaction_hash,
                    log_index,
                    contract_address,
                    user as operator,rewardToken as asset_address,reward
                    from `footprint-etl.ethereum_harvest.PotPool_event_RewardPaid`

                )
                where Date(block_timestamp) {match_date_filter}
            