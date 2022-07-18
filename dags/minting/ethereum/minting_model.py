from minting.minting_model import (
    MintingLiquidationModel as _MintingLiquidationModel,
    MintingBorrowModel as _MintingBorrowModel,
    MintingRepayModel as _MintingRepayModel,
    MintingSupplyModel as _MintingSupplyModel,
    MintingWithdrawModel as _MintingWithdrawModel,
)
from utils.constant import CHAIN


class MintingLiquidationModel(_MintingLiquidationModel):
    chain = CHAIN['ETHEREUM']


class MintingBorrowModel(_MintingBorrowModel):
    chain = CHAIN['ETHEREUM']


class MintingRepayModel(_MintingRepayModel):
    chain = CHAIN['ETHEREUM']


class MintingSupplyModel(_MintingSupplyModel):
    chain = CHAIN['ETHEREUM']


class MintingWithdrawModel(_MintingWithdrawModel):
    chain = CHAIN['ETHEREUM']
