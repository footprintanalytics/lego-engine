SELECT
            'MakerDAO' AS project,
            '2' AS version,
            15 AS protocol_id,
            'minting' As type,
            block_number,
            block_timestamp as block_time,
            transaction_hash as tx_hash,
            log_index,
            contract_address,
            borrower,
            m.stake_token[SAFE_OFFSET(0)] AS token_collateral_address,
            CAST(token_collateral_amount AS BIGNUMERIC) AS token_collateral_amount_raw,
            liquidated_borrower AS liquidator,
            '0x6b175474e89094c44da98b954eedeac495271d0f' AS repay_token_address,
            CAST(debt_to_cover_amount AS BIGNUMERIC) as repay_token_amount_raw,
            contract_address as pool_id
        FROM (
            -- Liquidation V1.0
            SELECT cat.block_number, cat.block_timestamp, cat.transaction_hash,
                   cat.flip AS contract_address, cat.log_index,
                   cat.art AS debt_to_cover_amount, flip.gal AS liquidated_borrower,
                   cat.ink AS token_collateral_amount, flip.usr as borrower
            FROM `footprint-etl.ethereum_maker.CAT_event_Bite` cat
            left join `footprint-etl.ethereum_maker.FLIP_event_Kick` flip
            on cat.id = flip.id and cat.transaction_hash = flip.transaction_hash
            WHERE CAST(cat.art as NUMERIC) > 0
            AND Date(cat.block_timestamp) {match_date_filter}
            AND flip.usr is not null

            UNION ALL

            -- Liquidation V2.0  0x8723b74f598de2ea49747de5896f9034cc09349e
            SELECT dog.block_number, dog.block_timestamp, dog.transaction_hash,
                   dog.clip AS contract_address, dog.log_index,
                   dog.art AS debt_to_cover_amount, clip.kpr AS liquidated_borrower,
                   dog.ink AS token_collateral_amount, clip.usr AS borrower
            FROM `footprint-etl.ethereum_maker.Dog_event_Bark` dog
            left join `footprint-etl.ethereum_maker.Clipper_event_Kick` clip
            on dog.id = clip.id and dog.transaction_hash = clip.transaction_hash
            WHERE CAST(dog.art as NUMERIC) > 0
            AND Date(dog.block_timestamp) {match_date_filter}
            AND clip.usr is not null
        ) liquidation
        LEFT JOIN `footprint-etl.footprint_pool_infos.pool_infos` m
        ON lower(liquidation.contract_address) = lower(m.pool_id)
        and m.protocol_id = 15