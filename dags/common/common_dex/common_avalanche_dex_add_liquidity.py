from .common_dex_add_liquidity import CommonDexAddLiquidity


class CommonAvalancheDexAddLiquidity(CommonDexAddLiquidity):
    transaction_dataset = 'footprint-blockchain-etl.crypto_avalanche.transactions'