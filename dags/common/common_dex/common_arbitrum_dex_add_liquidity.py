from .common_dex_add_liquidity import CommonDexAddLiquidity


class CommonArbitrumDexAddLiquidity(CommonDexAddLiquidity):
    transaction_dataset = 'footprint-blockchain-etl.crypto_arbitrum.transactions'