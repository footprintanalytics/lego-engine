from .common_dex_remove_liquidity import CommonDexRemoveLiquidity


class CommonPolygonDexRemoveLiquidity(CommonDexRemoveLiquidity):
    transaction_dataset = 'footprint-blockchain-etl.crypto_polygon.transactions'
