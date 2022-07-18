from .common_dex_add_liquidity import CommonDexAddLiquidity


class CommonBscDexAddLiquidity(CommonDexAddLiquidity):
    transaction_dataset = 'footprint-blockchain-etl.crypto_bsc.transactions'
