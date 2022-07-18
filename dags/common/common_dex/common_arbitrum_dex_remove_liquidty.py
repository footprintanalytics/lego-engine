from .common_dex_remove_liquidity import CommonDexRemoveLiquidity


class CommonArbitrumDexRemoveLiquidity(CommonDexRemoveLiquidity):
    transaction_dataset = 'footprint-blockchain-etl.crypto_arbitrum.transactions'
