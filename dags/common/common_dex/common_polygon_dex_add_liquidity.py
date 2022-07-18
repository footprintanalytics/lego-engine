from .common_dex_add_liquidity import CommonDexAddLiquidity


class CommonPolygonDexAddLiquidity(CommonDexAddLiquidity):
    transaction_dataset = 'footprint-blockchain-etl.crypto_polygon.transactions'

