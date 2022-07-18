from .common_dex_remove_liquidity import CommonDexRemoveLiquidity


class CommonBscDexRemoveLiquidity(CommonDexRemoveLiquidity):
    transaction_dataset = 'footprint-blockchain-etl.crypto_bsc.transactions'
