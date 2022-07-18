from web3 import Web3
import math
import pandas as pd
from eth_abi import encode_single, decode_single
from eth_utils import function_signature_to_4byte_selector, to_checksum_address
from typing import List
from web3.auto import w3
from enum import IntEnum
from decimal import Decimal
import tqdm


class Network(IntEnum):
    Mainnet = 1
    Kovan = 42
    Rinkeby = 4
    Görli = 5
    xDai = 100


MULTICALL_ADDRESSES = {
    Network.Mainnet: '0xeefBa1e63905eF1D7ACbA5a8513c70307C1cE441',
    Network.Kovan: '0x2cc8688C5f75E365aaEEb4ea8D6a480405A48D2A',
    Network.Rinkeby: '0x42Ad527de7d4e9d9d011aC45B31D8551f8Fe9821',
    Network.Görli: '0x77dCa2C955b15e9dE4dbBCf1246B4B85b651e50e',
    Network.xDai: '0xb5b692a88BDFc81ca69dcB1d924f59f0413A602a',
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
            'aggregate((address,bytes)[])(uint256,bytes[])',
            returns=None,
            _w3=self.w3,
            block_id=self.block_id
        )
        args = [[[call.target, call.data] for call in self.calls]]
        block, outputs = aggregate(args)
        result = {}
        for call, output in zip(self.calls, outputs):
            result.update(call.decode_output(output))
        return result


class Web3Utils:

    def __init__(self, provider_type, provider):
        if provider_type == 'ipc':
            self.w3 = Web3(Web3.IPCProvider(provider))
        elif provider_type == 'http':
            self.w3 = Web3(Web3.HTTPProvider(provider))
        elif provider_type == 'websocket':
            self.w3 = Web3(Web3.WebsocketProvider(provider))
        else:
            print('w3 provider error')

    def decimal_factory(self):
        def wrap(value):
            return Decimal(value)

        return wrap

    def get_token_balance_by_pool_info(self, info=list()):
        calls = [
            Call(item.get('token_address'),
                 ['balanceOf(address)(uint256)', Web3.toChecksumAddress(item.get('contract_address'))],
                 [['{}_{}'.format(item.get('contract_address'), item.get('token_address')), self.decimal_factory()]])
            for item in info
        ]

        multi = Multicall(calls, _w3=self.w3)
        result = multi()
        return result

    def multi_call_limit_200(self, info = list()):
        multi_call_limit = 200
        loop_count = math.ceil(len(info) / multi_call_limit)
        return [self.get_token_balance_by_pool_info(info[multi_call_limit * index: multi_call_limit * (index + 1)]) for
                index in tqdm.tqdm(range(loop_count))]

        # _contract_address = Web3.toChecksumAddress(contract_address)
        # _token_address = Web3.toChecksumAddress(token_address)
        #
        # token = self.w3.eth.contract(abi=token_address_abi, address=_token_address)
        #
        # balance = token.functions.balanceOf(_contract_address).call()
        # decimals = token.functions.decimals().call()
        # return balance / math.pow(10, decimals)
