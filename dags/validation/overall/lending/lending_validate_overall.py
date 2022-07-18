from validation.inner_validate.lending.lending_asset_borrow_less_than_repay import LendingAssetBorrowLessThanRepay
from validation.inner_validate.lending.lending_asset_liquidation_not_supply import LendingAssetLiquidationNotSupply
from validation.inner_validate.lending.lending_net_asset_less_than_zero import LendingNetAssetLessThanZero
from validation.inner_validate.lending.lending_pool_asset_borrow_less_than_repay import \
    LendingPoolAssetBorrowLessThanRepay
from validation.inner_validate.lending.lending_pool_net_asset_less_than_zero import LendingPoolNetAssetLessThanZero
from validation.overall.base_validate_overall import BaseValidateOverall


class LendingValidateOverall(BaseValidateOverall):
    business_type = 'lending'

    def inner(self):
        return [
            {
                'cls': LendingAssetBorrowLessThanRepay,
                'config': {
                    'tolerance': 0.05
                }
            },
            {
                'cls': LendingNetAssetLessThanZero,
                'config': {}
            },
            {
                'cls': LendingAssetLiquidationNotSupply,
                'config': {}
            },
            {
                'cls': LendingPoolAssetBorrowLessThanRepay,
                'config': {}
            },
            {
                'cls': LendingPoolNetAssetLessThanZero,
                'config': {}
            }
        ]

    def outside(self):
        return []


if __name__ == '__main__':
    ins = LendingValidateOverall({'chain': 'ethereum', 'project': 'aave', 'business_type': 'lending'})
    ins.process()
