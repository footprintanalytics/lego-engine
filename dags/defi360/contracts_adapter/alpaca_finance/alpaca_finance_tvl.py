import pandas
import pydash
import json
import os

from config import project_config
from defi360.utils.multicall import load_abi, multi_call
from defi360.contracts_adapter.token_balances.token_balances_dodo import TokenBalancesDODO

class AlpacaFinanceTVL(TokenBalancesDODO):
    execution_time = "30 2 * * *"
    history_date = "2022-03-12"
    schema_name = "defi360/schema/token_balances.json"
    depend_sql_path = 'defi360/contracts_adapter/alpaca_finance/alpaca_pools.sql'
    history_day = 5

    def __init__(self):
        super(AlpacaFinanceTVL, self).__init__()
        dags_folder = project_config.dags_folder
        mainnet_file = open(os.path.join(dags_folder, 'defi360/contracts_adapter/alpaca_finance/bsc_mainnet.json'))
        self.addresses = json.loads(mainnet_file.read())
        abi_file = open(os.path.join(dags_folder, 'defi360/contracts_adapter/abi/alpaca-finance.json'))
        self.abis = json.loads(abi_file.read())

    def get_work_holding(self, vaults_pools: list,chain='BSC', block_number: int = None):
        addresses = self.addresses
        abis = self.abis
        values = {
            'chain': chain
        }
        for vault_address in vaults_pools:
            user_info_abi = abis['userInfo']
            print(vault_address)
            vault = pydash.filter_(addresses["Vaults"], lambda address: str(address['address']).lower() == vault_address)
            workers = vault[0]['workers']
            # 过滤剩下单币的worker
            singeAsset_worker = pydash.filter_(workers, lambda worker: str(worker['name']).find('CakeMaxiWorker') >= 0)

            token_address = pydash.get(singeAsset_worker, 'stakingToken')
            calls = pydash.map_(workers, lambda worker: {
                'target': worker['stakingTokenAt'],
                'params': [worker["pId"], worker["address"]]
            })
            # 获取所以worker持有的amount
            res = multi_call(calls, user_info_abi, block_number, chain)
            worker_map = pydash.map_(res, lambda data: {
                'address': data['params'][1],
                'amount': data['output']['amount'],
                'token_address': token_address
            })
            values[vault_address] = worker_map
        print(values)
        return values

    def get_borrow(self, vaults_pools: list, chain='BSC', block_number: int = None):
        addresses = self.addresses
        abis = self.abis
        values = {
            'chain': chain
        }
        for vault_address in vaults_pools:
            vault_debt_abi = abis['vaultDebtVal']
            vault = pydash.filter_(addresses["Vaults"],
                                   lambda address: str(address['address']).lower() == vault_address)
            token_address = pydash.get(vault[0], 'baseToken')
            calls = [
                {
                    'target': vault_address
                }
            ]
            print(token_address)
            res = multi_call(calls, vault_debt_abi, block_number, chain)
            print(res)
            borrow_map = pydash.map_(res, lambda data: {
                'address': vault_address,
                'amount': data['output'],
                'token_address': token_address
            })
            values[vault_address] = borrow_map
        print(values)
        return values

    def get_unused_btoken(self, vaults_pools: list, chain='BSC', block_number: int = None):
        addresses = self.addresses
        abis = self.abis
        values = {
            "chain": chain
        }
        for vault_address in vaults_pools:
            vault = pydash.filter_(addresses["Vaults"], lambda address: str(address['address']).lower() == vault_address)
            token_address = pydash.get(vault[0], 'baseToken')
            balance_of_abi = abis['balanceOf']
            calls = [
                {
                    'target': vault[0]['baseToken'],
                    'params': vault[0]['address']
                }
            ]
            print(token_address)
            res = multi_call(calls, balance_of_abi, block_number, chain)
            unused_map = pydash.map_(res, lambda data: {
                'address': vault_address,
                'amount': data['output'],
                'token_address': token_address
            })
            values[vault_address] = unused_map
        print(values)
        return values

class AlpacaFinancePoolBalance(AlpacaFinanceTVL):
    data_set = "public_data"
    task_name = "token_balances_alpaca_finance"

    def get_daily_data(self, run_date: str, block_numbers: list):
        block_numbers = self.get_block_number(run_date)
        pools_data = self.get_depend_data_with_cache(run_date)
        tvl = []
        for chain, pools in pools_data.groupby('chain'):
            for row_index, pool in pools.iterrows():
                pools = {}
                pools_address = [pool.deposit_address]
                block_number = pydash.get(pydash.find(block_numbers, {"chain": chain.lower(), "date": run_date}),
                                          "block")
                unused_tvl = self.get_unused_btoken(pools_address, chain, block_number)
                pools[pools_address[0]] = unused_tvl[pools_address[0]]
                for i in pools[pools_address[0]]:
                    value = {
                        "chain": chain,
                        "holder_address": i['address'],
                        "token_address": i['token_address'],
                        "day": run_date,
                        "balance": i['amount']
                    }
                    tvl.append(value)
        df = pandas.DataFrame(tvl)
        return df

# todo 方法复用了
class AlpacaFinancePoolBorrow(AlpacaFinanceTVL):
    data_set = "gaia_dao"
    task_name = "borrow_token_balance_alpaca_finance"

    def get_daily_data(self, run_date: str, block_numbers: list):
        block_numbers = self.get_block_number(run_date)
        pools_data = self.get_depend_data_with_cache(run_date)
        tvl = []
        for chain, pools in pools_data.groupby('chain'):
            for row_index, pool in pools.iterrows():
                pools = {}
                pools_address = [pool.deposit_address]
                block_number = pydash.get(pydash.find(block_numbers, {"chain": chain.lower(), "date": run_date}),
                                          "block")
                unused_tvl = self.get_borrow(pools_address, chain, block_number)
                pools[pools_address[0]] = unused_tvl[pools_address[0]]
                for i in pools[pools_address[0]]:
                    value = {
                        "chain": chain,
                        "holder_address": i['address'],
                        "token_address": i['token_address'],
                        "day": run_date,
                        "balance": i['amount']
                    }
                    tvl.append(value)
        df = pandas.DataFrame(tvl)
        return df

if __name__ == '__main__':
    a = AlpacaFinancePoolBalance()
    # b = AlpacaFinancePoolBorrow()

    # a.load_daily_data()
    # b.load_daily_data()

    a.load_history_data()
    # b.load_history_data()

