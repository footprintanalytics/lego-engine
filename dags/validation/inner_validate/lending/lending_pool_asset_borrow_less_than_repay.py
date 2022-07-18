from validation.inner_validate.base_inner_validate import BaseInnerValidate
import pydash

from validation.inner_validate.query import lending_pool_asset_sum, group_by_pool_asset


class LendingPoolAssetBorrowLessThanRepay(BaseInnerValidate):
    borrow_table = None
    repay_table = None
    tolerance = 0.1

    def __init__(self, cfg):
        super().__init__(cfg)
        if 'tolerance' in cfg:
            self.tolerance = cfg['tolerance']
        self.borrow_table = 'footprint-etl.{chain}_{business_type}_{project}.{project}_{business_type}_borrow_all'.format(
            chain=self.chain, project=self.project, business_type=self.business_type)
        self.repay_table = 'footprint-etl.{chain}_{business_type}_{project}.{project}_{business_type}_repay_all'.format(
            chain=self.chain, project=self.project, business_type=self.business_type)
        self.add_validate_table(self.borrow_table)
        self.add_validate_table(self.repay_table)

    def get_self_data(self):
        borrow = lending_pool_asset_sum(self.chain, self.project, 'borrow')
        repay = lending_pool_asset_sum(self.chain, self.project, 'repay')
        return {'borrow': borrow, 'repay': repay}

    def check_rule(self, values):
        borrow = values['borrow']
        repay = values['repay']
        data_map = group_by_pool_asset([
            {'name': 'borrow', 'data': borrow},
            {'name': 'repay', 'data': repay},
        ])

        result_code = 0
        result_message = 'borrow大于repay校验通过'
        success = []
        fail = []
        for pool, pool_info in data_map.items():
            for asset, asset_info in pool_info.items():
                print('asset_info {}'.format(asset_info))
                if pydash.get(asset_info, 'borrow', 0) * (1 + self.tolerance) < pydash.get(asset_info, 'repay', 0):
                    fail.append({'pool': pool, 'asset_token': asset})
                else:
                    success.append({'pool': pool, 'asset_token': asset})

        if len(fail) > 0:
            result_code = 1
            result_message = 'borrow大于repay校验失败 错误信息: {} '.format(fail)
        return {
            "result_code": result_code,
            "result_message": result_message,
            "validate_data": {
                'query_data': data_map
            }
        }


if __name__ == '__main__':
    v = LendingPoolAssetBorrowLessThanRepay({
        "chain": "ethereum",
        "business_type": "lending",
        "project": "compound",
        "tolerance": 0.1
    })

    v.validate()
