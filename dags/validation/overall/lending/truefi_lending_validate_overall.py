from validation.inner_validate.lending.lending_asset_borrow_less_than_repay import LendingAssetBorrowLessThanRepay
from validation.inner_validate.lending.lending_asset_usage_rate import LendingAssetUsageRate
from validation.inner_validate.lending.lending_net_asset_less_than_zero import LendingNetAssetLessThanZero
from validation.overall.lending import LendingValidateOverall


class TruefiLendingValidateOverall(LendingValidateOverall):

    def inner(self):
        return [
            {
                'cls': LendingAssetBorrowLessThanRepay,
                'config': {}
            },
            {
                'cls': LendingNetAssetLessThanZero,
                'config': {
                    'ignore_asset': ['0xdf5e0e81dff6faf3a7e52ba697820c5e32d806a8']
                }
            },
            {
                'cls': LendingAssetUsageRate,
                'config': {
                    'pledge_rate': 0.35
                }
            }
        ]


if __name__ == '__main__':
    ins = TruefiLendingValidateOverall({'chain': 'ethereum', 'project': 'truefi', 'business_type': 'lending'})
    ins.process()
