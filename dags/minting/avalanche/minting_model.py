from minting.minting_model import (
    MintingLiquidationModel as _MintingLiquidationModel,
    MintingBorrowModel as _MintingBorrowModel,
    MintingRepayModel as _MintingRepayModel,
    MintingSupplyModel as _MintingSupplyModel,
    MintingWithdrawModel as _MintingWithdrawModel,
)
from utils.constant import CHAIN


class MintingLiquidationModel(_MintingLiquidationModel):
    chain = CHAIN['AVALANCHE']

    def __init__(self):
        if 'avalanche_' not in self.task_name:
            self.task_name = 'avalanche_' + self.task_name
        if 'avalanche_' not in self.project_name:
            self.project_name = 'avalanche_' + self.project_name
        super().__init__()


class MintingBorrowModel(_MintingBorrowModel):
    chain = CHAIN['AVALANCHE']

    def __init__(self):
        if 'avalanche_' not in self.task_name:
            self.task_name = 'avalanche_' + self.task_name
        if 'avalanche_' not in self.project_name:
            self.project_name = 'avalanche_' + self.project_name
        super().__init__()


class MintingRepayModel(_MintingRepayModel):
    chain = CHAIN['AVALANCHE']

    def __init__(self):
        if 'avalanche_' not in self.task_name:
            self.task_name = 'avalanche_' + self.task_name
        if 'avalanche_' not in self.project_name:
            self.project_name = 'avalanche_' + self.project_name
        super().__init__()


class MintingSupplyModel(_MintingSupplyModel):
    chain = CHAIN['AVALANCHE']

    def __init__(self):
        if 'avalanche_' not in self.task_name:
            self.task_name = 'avalanche_' + self.task_name
        if 'avalanche_' not in self.project_name:
            self.project_name = 'avalanche_' + self.project_name
        super().__init__()


class MintingWithdrawModel(_MintingWithdrawModel):
    chain = CHAIN['AVALANCHE']

    def __init__(self):
        if 'avalanche_' not in self.task_name:
            self.task_name = 'avalanche_' + self.task_name
        if 'avalanche_' not in self.project_name:
            self.project_name = 'avalanche_' + self.project_name
        super().__init__()
