from validation.inner_validate.base_inner_validate import BaseInnerValidate
import pydash

from validation.inner_validate.query import lending_asset_sum, group_by_asset


class LendingAssetUsageRate(BaseInnerValidate):
    # 忽略计算的资产
    ignore_asset = []
    pledge_rate = 0.7

    def __init__(self, cfg):
        super().__init__(cfg)
        if 'ignore_asset' in cfg:
            self.ignore_asset = cfg['ignore_asset']
        for t in ['borrow', 'repay', 'supply', 'withdraw']:
            self.add_validate_table(
                'footprint-etl.{chain}_{business_type}_{project}.{project}_{business_type}_{t}_all'.format(
                    chain=self.chain, project=self.project, business_type=self.business_type, t=t))
        if 'pledge_rate' in cfg:
            self.pledge_rate = cfg['pledge_rate']

    def get_self_data(self):
        borrow = lending_asset_sum(self.chain, self.project, 'borrow')
        repay = lending_asset_sum(self.chain, self.project, 'repay')
        supply = lending_asset_sum(self.chain, self.project, 'supply')
        withdraw = lending_asset_sum(self.chain, self.project, 'withdraw')
        return {'borrow': borrow, 'repay': repay, 'supply': supply, 'withdraw': withdraw}

    def check_rule(self, values):
        borrow = values['borrow']
        repay = values['repay']
        supply = values['supply']
        withdraw = values['withdraw']
        data_map = group_by_asset([
            {'name': 'borrow', 'data': borrow},
            {'name': 'repay', 'data': repay},
            {'name': 'supply', 'data': supply},
            {'name': 'withdraw', 'data': withdraw},
        ])

        asset_ls = pydash.keys(data_map)

        ls = pydash.map_(asset_ls, lambda x: {
            'asset': x,
            'rate': (
                            pydash.get(data_map, x + '.borrow', 0) -
                            pydash.get(data_map, x + '.repay', 0)) / (
                            pydash.get(data_map, x + '.supply', 0) -
                            pydash.get(data_map, x + '.withdraw', 0))
        })
        ls = pydash.sort_by(ls, lambda x: x['rate'])
        print('123')

        result_code = 0
        result_message = '资产利用率校验通过'
        asset_use_rate = pydash.map_(asset_ls, lambda x: {
            'asset': x,
            'use_rate': (
                            pydash.get(data_map, x + '.borrow', 0) -
                            pydash.get(data_map, x + '.repay', 0)) / (
                            pydash.get(data_map, x + '.supply', 0) -
                            pydash.get(data_map, x + '.withdraw', 0)
                    )
        })
        err_asset = pydash.find(asset_use_rate,
                                lambda x: x['asset'] not in self.ignore_asset and x['use_rate'] < self.pledge_rate
                                )
        if err_asset is not None:
            result_code = 1
            result_message = '资产利用率校验 错误Asset:{} borrow:{} repay:{} supply:{} withdraw:{} 预期使用率:{} 实际使用率:{}'.format(
                err_asset,
                pydash.get(data_map[err_asset], 'borrow', None),
                pydash.get(data_map[err_asset], 'repay', None),
                pydash.get(data_map[err_asset], 'supply', None),
                pydash.get(data_map[err_asset], 'withdraw', None),
                self.pledge_rate,
                (pydash.get(data_map[err_asset], 'borrow', 0) - pydash.get(data_map[err_asset], 'repay', 0)) / (
                        pydash.get(data_map[err_asset], 'supply', 0) - pydash.get(data_map[err_asset], 'withdraw',
                                                                                  0))
            )
        return {
            "result_code": result_code,
            "result_message": result_message,
            "validate_data": {
                'query_data': data_map
            }
        }


if __name__ == '__main__':
    v = LendingAssetUsageRate({
        "chain": "ethereum",
        "business_type": "lending",
        "project": "compound",
    })

    v.validate()
