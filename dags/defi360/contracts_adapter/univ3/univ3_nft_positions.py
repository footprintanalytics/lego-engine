import pandas as pd
import pydash

from defi360.contracts_adapter.token_balances.token_balances_dodo import TokenBalancesDODO
from defi360.utils.multicall import load_abi, multi_call


class UNIV3NFTPositions(TokenBalancesDODO):
    data_set = "gaia_dao"
    task_name = "univ3_nft_positions"
    execution_time = "30 2 * * *"
    history_date = "2022-02-19"
    schema_name = "defi360/schema/univ3_nft_positions.json"
    depend_sql_path = 'defi360/contracts_adapter/univ3/depend_data.sql'
    history_day = 60

    def get_positions(self, pools, chain: str = 'Ethereum', run_date=None, block_number: int = None):
        if block_number is None:
            raise Exception('block_number 不允许为 None')

        token_ids = pools['token_id'].tolist()
        # token_ids = token_ids[0:10]
        print('debug token_ids ', chain, len(token_ids))
        univ3_abi = load_abi('defi360/contracts_adapter/abi/univ3.json', 'positions')
        UNIV3_NFT_ADDRSS = '0xc36442b4a4522e871399cd717abdd847ab11fe88'  # eth
        calls = pydash.map_(token_ids, lambda td: {
            'target': UNIV3_NFT_ADDRSS,
            'params': int(td),
        })
        print('get_positions debug calls ', chain, len(calls))
        res = multi_call(calls, univ3_abi, block_number, chain)
        positions = []
        for item in res:
            if item['output'] is None:
                continue
            positions.append(
                {
                    'tokenId': item['params'][0],
                    'date': run_date,
                    'chain': chain,
                    **item['output']
                }
            )
        return positions

    # 获取合约取数数据到csv
    def get_daily_data(self, run_date: str, block_numbers: list):
        pools_data = self.get_depend_data_with_cache(run_date)
        # print(pools_data)
        token_balances = []
        for chain, pools in pools_data.groupby('chain'):
            block_number = pydash.get(pydash.find(block_numbers, {"chain": chain.lower(), "date": run_date}), "block")
            tbs = self.get_positions(pools, chain, run_date, block_number)
            token_balances += tbs
        df = pd.DataFrame(token_balances)
        return df


if __name__ == '__main__':
    job = UNIV3NFTPositions()

    # 跑历史数据
    # job.load_history_data(debug=True)

    # 跑当天数据
    job.load_daily_data(debug=True, run_date="2022-02-09")

    # # 合并视图
    # templateSqlUpload.create_data_view()

    # print('upload done')
