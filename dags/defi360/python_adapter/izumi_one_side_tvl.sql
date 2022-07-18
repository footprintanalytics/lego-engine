select
    Date(block_timestamp) as day,
    chain,
    contract_address,
    sum(
        case when business_type="erc20Supply" then ifnull(token_amount,0)
        when business_type="erc20Withdraw" then -1*ifnull(token_amount,0) end

    ) as token_amount from
    `gaia-data.gaia.izumi_one_side` group by day,chain,contract_address order by 1