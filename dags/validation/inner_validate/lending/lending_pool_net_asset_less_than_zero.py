from validation.inner_validate.base_inner_validate import BaseInnerValidate
import pydash

from validation.inner_validate.query import lending_pool_asset_sum, group_by_pool_asset, lending_pool_liquidation_repay, \
    lending_pool_liquidation_collateral


class LendingPoolNetAssetLessThanZero(BaseInnerValidate):
    # 忽略计算的资产
    ignore_asset = []
    tolerance = 0

    def __init__(self, cfg):
        super().__init__(cfg)
        if 'tolerance' in cfg:
            self.tolerance = cfg['tolerance']
        if 'ignore_asset' in cfg:
            self.ignore_asset = cfg['ignore_asset']
        for t in ['borrow', 'repay', 'supply', 'withdraw']:
            self.add_validate_table(
                'footprint-etl.{chain}_{business_type}_{project}.{project}_{business_type}_{t}_all'.format(
                    chain=self.chain, project=self.project, business_type=self.business_type, t=t))

    def get_self_data(self):
        borrow = lending_pool_asset_sum(self.chain, self.project, 'borrow')
        repay = lending_pool_asset_sum(self.chain, self.project, 'repay')
        supply = lending_pool_asset_sum(self.chain, self.project, 'supply')
        withdraw = lending_pool_asset_sum(self.chain, self.project, 'withdraw')
        liquidation_repay = lending_pool_liquidation_repay(self.chain, self.project)
        liquidation_collateral = lending_pool_liquidation_collateral(self.chain, self.project)
        return {'borrow': borrow, 'repay': repay, 'supply': supply, 'withdraw': withdraw,
                'liquidation_repay': liquidation_repay, 'liquidation_collateral': liquidation_collateral}

    def check_rule(self, values):
        borrow = values['borrow']
        repay = values['repay']
        supply = values['supply']
        withdraw = values['withdraw']
        liquidation_repay = values['liquidation_repay']
        liquidation_collateral = values['liquidation_collateral']
        data_map = group_by_pool_asset([
            {'name': 'borrow', 'data': borrow},
            {'name': 'repay', 'data': repay},
            {'name': 'supply', 'data': supply},
            {'name': 'withdraw', 'data': withdraw},
            {'name': 'liquidation_repay', 'data': liquidation_repay},
            {'name': 'liquidation_collateral', 'data': liquidation_collateral},
        ])

        result_code = 0
        result_message = '池子净资产校验通过'
        success = []
        fail = []
        for pool, pool_info in data_map.items():
            for asset, asset_info in pool_info.items():
                print('asset_info {}:  {}'.format(pool, asset_info))
                s = pydash.get(pool_info, 'supply', 0)
                w = pydash.get(pool_info, 'withdraw', 0)
                b = pydash.get(pool_info, 'borrow', 0)
                r = pydash.get(pool_info, 'repay', 0)
                l_r = pydash.get(pool_info, 'liquidation_repay', 0)
                l_c = pydash.get(pool_info, 'liquidation_collateral', 0)
                net_asset = (s - w) - (b - r) + l_r - l_c
                if asset not in self.ignore_asset and net_asset < 0:
                    fail.append({'pool': pool, 'asset_token': asset, 'net_asset': net_asset})
                else:
                    success.append({'pool': pool, 'asset_token': asset, 'net_asset': net_asset})

        if len(fail) > 0:
            result_code = 1
            result_message = '池子净资产校验失败 pool:{} asset_token:{} net_asset:{}'.format(
                fail[0].get('pool'),
                fail[0].get('asset_token'),
                fail[0].get('net_asset'),
            )
        return {
            "result_code": result_code,
            "result_message": result_message,
            "validate_data": {
                'query_data': data_map
            }
        }


if __name__ == '__main__':
    v = LendingPoolNetAssetLessThanZero({
        "chain": "ethereum",
        "business_type": "lending",
        "project": "aave",
        "tolerance": 0
    })

    v.validate()
