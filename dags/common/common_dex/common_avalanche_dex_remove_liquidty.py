from .common_dex_remove_liquidity import CommonDexRemoveLiquidity


class CommonAvalancheDexRemoveLiquidity(CommonDexRemoveLiquidity):
    transaction_dataset = 'footprint-blockchain-etl.crypto_avalanche.transactions'
