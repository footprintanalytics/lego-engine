from validation.inner_validate.base_inner_validate import BaseInnerValidate
import pydash

from validation.inner_validate.query import lending_asset_sum, group_by_asset


class LendingAssetBorrowLessThanRepay(BaseInnerValidate):
    borrow_table = None
    repay_table = None
    tolerance = 0

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
        borrow = lending_asset_sum(self.chain, self.project, 'borrow')
        repay = lending_asset_sum(self.chain, self.project, 'repay')
        return {'borrow': borrow, 'repay': repay}

    def check_rule(self, values):
        borrow = values['borrow']
        repay = values['repay']
        data_map = group_by_asset([
            {'name': 'borrow', 'data': borrow},
            {'name': 'repay', 'data': repay},
        ])

        asset_ls = pydash.keys(data_map)

        result_code = 0
        result_message = 'borrow大于repay校验通过'
        err_asset = pydash.find(asset_ls, lambda x: pydash.get(data_map, x + 'borrow', 0) * (1 + self.tolerance) <
                                                    pydash.get(data_map, x + 'repay', 0))
        if err_asset is not None:
            result_code = 1
            result_message = 'borrow大于repay校验失败 错误Asset:{} borrow:{} repay:{}'.format(err_asset,
                                                                                      data_map[err_asset]['borrow'],
                                                                                      data_map[err_asset]['repay'],
                                                                                      )
        return {
            "result_code": result_code,
            "result_message": result_message,
            "validate_data": {
                'query_data': data_map
            }
        }


if __name__ == '__main__':
    v = LendingAssetBorrowLessThanRepay({
        "chain": "ethereum",
        "business_type": "lending",
        "project": "compound",
        "tolerance": 0
    })

    v.validate()
