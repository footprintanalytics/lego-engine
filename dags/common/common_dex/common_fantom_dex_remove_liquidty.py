from .common_dex_remove_liquidity import CommonDexRemoveLiquidity


class CommonFantomDexRemoveLiquidity(CommonDexRemoveLiquidity):
    transaction_dataset = 'footprint-blockchain-etl.crypto_fantom.transactions'
