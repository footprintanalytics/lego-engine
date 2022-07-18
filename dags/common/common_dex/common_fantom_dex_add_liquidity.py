from .common_dex_add_liquidity import CommonDexAddLiquidity


class CommonFantomDexAddLiquidity(CommonDexAddLiquidity):
    transaction_dataset = 'footprint-blockchain-etl.crypto_fantom.transactions'