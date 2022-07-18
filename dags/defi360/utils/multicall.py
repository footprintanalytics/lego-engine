import json
import os
from enum import IntEnum
from typing import List

import pydash
from eth_abi import encode_single, decode_single
from eth_utils import function_signature_to_4byte_selector
from eth_utils import to_checksum_address
from web3 import Web3
from web3.auto import w3

from config import project_config
from utils.query_bigquery import query_bigquery

hasura_url = 'https://hasura-prod.internal.footprint.network/v1/graphql'
import math

class Network(IntEnum):
    Mainnet = 1
    Kovan = 42
    Rinkeby = 4
    Görli = 5
    Avalanche = 2222
    xDai = 100
    Polygon = 137
    Bsc = 56
    Fantom = 250
    Heco = 128
    Arbitrum = 42161


# v2
MULTICALL_ADDRESSES = {
    Network.Mainnet: '0x5ba1e12693dc8f9c48aad8770482f4739beed696',
    Network.Polygon: '0x275617327c958bD06b5D6b871E7f491D76113dd8',
    Network.Bsc: '0xed386Fe855C1EFf2f843B910923Dd8846E45C5A4',
    Network.Fantom: '0xD98e3dBE5950Ca8Ce5a4b59630a5652110403E5c',
    Network.Avalanche: '0x29b6603d17b9d8f021ecb8845b6fd06e1adf89de', # https://snowtrace.io/address/0x29b6603d17b9d8f021ecb8845b6fd06e1adf89de#code
    Network.Arbitrum: '0x80C7DD17B01855a6D2347444a0FCC36136a314de',
    # Network.Heco: '0xc9a9F768ebD123A00B52e7A0E590df2e9E998707',
    # 其他链可以去 pr 里找找，有惊喜 https://github.com/makerdao/multicall/pulls
    # 或者 google Multicall2 site: ethscan
}

CHAIN_RPC = {
    'Ethereum': 'https://small-damp-tree.quiknode.pro/b9c72a1e111bed57e6029bfeb003eddb0e73bb70/',
    # 'Ethereum': 'https://eth-mainnet.alchemyapi.io/v2/XF_w-nJIEOkjy6f2Ea-RvGUozfB0egVy',
    # "Polygon": 'https://polygon-rpc.com',
    "Polygon": 'https://wispy-bitter-moon.matic.quiknode.pro/2f57b9740c25868fe400d6dcf942ce49aa78bef0/',
    "BSC": "https://ancient-frosty-surf.bsc.quiknode.pro/ad8136ad2737aa28ed5097f50dee6379a29a09c3/",
    "Arbitrum": "https://old-wispy-lake.arbitrum-mainnet.quiknode.pro/f6269c559441ce2c25614b128d72e86509c4fd6b/"
    # "BSC": "https://bsc-dataseed.binance.org/"
}


def parse_signature(signature):
    """
    Breaks 'func(address)(uint256)' into ['func', '(address)', '(uint256)']
    """
    parts = []
    stack = []
    start = 0
    for end, letter in enumerate(signature):
        if letter == '(':
            stack.append(letter)
            if not parts:
                parts.append(signature[start:end])
                start = end
        if letter == ')':
            stack.pop()
            if not stack:  # we are only interested in outermost groups
                parts.append(signature[start:end + 1])
                start = end + 1
    return parts


class BlockNumber:

    @staticmethod
    def get_bulk_block_number(start_date: str, end_date: str):
        sql = """
            SELECT
              *
            FROM
              `xed-project-237404.gaia_dao.date_block`
            WHERE
              DATE(date) <= "{end_date}"
              AND DATE(date) >= "{start_date}"
        """.format(end_date=end_date, start_date=start_date)
        block_number = query_bigquery(sql).to_dict('records')
        return block_number


class Signature:
    def __init__(self, signature):
        self.signature = signature
        self.parts = parse_signature(signature)
        self.input_types = self.parts[1]
        self.output_types = self.parts[2]
        self.function = ''.join(self.parts[:2])
        self.fourbyte = function_signature_to_4byte_selector(self.function)

    def encode_data(self, args=None):
        return self.fourbyte + encode_single(self.input_types, args) if args else self.fourbyte

    def decode_data(self, output):
        return decode_single(self.output_types, output)


class Call:
    def __init__(self, target, function, returns=None, _w3=None, block_id=None):
        self.target = to_checksum_address(target)

        if isinstance(function, list):
            self.function, *self.args = function
        else:
            self.function = function
            self.args = None

        if _w3 is None:
            self.w3 = w3
        else:
            self.w3 = _w3

        self.signature = Signature(self.function)
        self.returns = returns
        self.block_id = block_id

    @property
    def data(self):
        return self.signature.encode_data(self.args)

    # def decode_result(self, output):
    #     '''
    #     convert tuple result to list
    #     :param output:
    #     :return:
    #     '''
    #     decoded = self.signature.decode_data(output)
    #     if type(decoded) is tuple:
    #         return [e for e in decoded[0]] if decoded and len(decoded[0]) else []
    #     else:
    #         return decoded if len(decoded) > 1 else decoded[0]

    def decode_output(self, output):
        decoded = self.signature.decode_data(output)
        if self.returns:
            return {
                name: handler(value) if handler else value
                for (name, handler), value
                in zip(self.returns, decoded)
            }
        else:
            return decoded if len(decoded) > 1 else decoded[0]

    def __call__(self, args=None):
        args = args or self.args
        calldata = self.signature.encode_data(args)
        output = self.w3.eth.call({'to': self.target, 'data': calldata}, block_identifier=self.block_id)
        return self.decode_output(output)


class Multicall:
    def __init__(self, calls: List[Call], _w3=None, block_id=None):
        self.calls = calls
        self.block_id = block_id
        if _w3 is None:
            self.w3 = w3
        else:
            self.w3 = _w3

    def __call__(self):
        aggregate = Call(
            MULTICALL_ADDRESSES[self.w3.eth.chainId],
            'tryAggregate(bool,(address,bytes)[])((bool,bytes)[])',
            returns=None,
            _w3=self.w3,
            block_id=self.block_id
        )
        args = [False, [[call.target, call.data] for call in self.calls]]
        outputs = aggregate(args)
        result = []
        for call, (success, output) in zip(self.calls, outputs):
            output_decoded = None
            if success:
                try:
                    output_decoded = call.decode_output(output)
                except Exception as e:
                    success = False
                    print('decode_output error: ', e)

            result.append({
                'input': call.target.lower(),
                'success': success,
                'output':  output_decoded,
                'params': call.args
            })
        return result


def abi_to_string(abi):
    name = pydash.get(abi, 'name')
    inputs = ','.join(pydash.map_(abi['inputs'], 'type'))
    outputs = ','.join(pydash.map_(abi['outputs'], 'type'))
    if len(abi['outputs']) > 1:
        outputs = f'({outputs})'
    abi_str = f'{name}({inputs})({outputs})'
    # print(abi_str)
    return abi_str


def decode_by_abi(res, abi):
    abi_outputs = pydash.get(abi, 'outputs')
    if not abi_outputs:
        return res
    if len(abi_outputs) < 2:
        return res

    for item in res:
        new_output = {}
        if item['output'] is None:
            continue
        for value, out_format in zip(item['output'], abi_outputs):
            # print('===>', value, out_format)
            new_output[out_format['name']] = value
        item['output'] = new_output
    return res


def load_abi(path, name):
    dags_folder = project_config.dags_folder
    json_path = os.path.join(dags_folder, path)
    with open(json_path) as f:
        info_json = json.load(f)
    all_abi = info_json
    abi = pydash.find(all_abi, {'name': name})
    return abi


def multi_call(calls: list, abi, block=None, chain='Ethereum'):
    calls_args = []
    rpc = CHAIN_RPC[chain]
    _w3 = Web3(
        Web3.HTTPProvider(rpc))  # main
    for c in calls:
        abi_str = abi_to_string(abi)
        try:
            params = c['params'] if isinstance(c['params'], list) else [c['params']]
            calls_args.append(Call(c['target'], [abi_str, *params]))
        except:
            calls_args.append(Call(c['target'], [abi_str]))

    print('debug multi_call {}, {} {}'.format(chain, block, len(calls)))
    # 每次执行长度
    data_len = 2000
    if chain == 'Polygon':
        data_len = 120
    # 循环次数
    round = math.ceil(len(calls) / data_len)
    # 存储结果
    res_list = []

    while round > 0:
        start = data_len*(round-1)
        end = data_len*round
        res = Multicall(calls=calls_args[start:end], _w3=_w3, block_id=block)()
        result = decode_by_abi(res, abi)
        res_list.extend(result)
        print('multi call result length: ', len(result))
        round -= 1

    return res_list
