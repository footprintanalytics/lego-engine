import math
import pydash
from web3 import Web3

from models import ProjectCreatorAddress, AbiInfo, AbiInfoInput

if __name__ == '__main__':
    address_count = ProjectCreatorAddress.count({})
    print('总数:{}'.format(address_count))
    limit = 1000
    times = math.ceil(address_count / limit)
    print('times:{}'.format(times))
    for i in range(times):
        print('i:{}/{}'.format(i, times))
        origin = ProjectCreatorAddress.find_list(query={}, projection={'tagName': 1, 'contractAddress': 1, '_id': 0},
                                                 limit=limit, skip=i * limit)
        all_address = pydash.map_(origin, 'contractAddress')
        all_abi = AbiInfo.find_list(query={'contract_address': {'$in': all_address}},
                                    projection={'abi': 1, 'contract_address': 1, '_id': 0})
        for item in origin:
            project = item['tagName']
            contractAddress = item['contractAddress']
            abi_info = pydash.find(all_abi, lambda abi: abi['contract_address'].lower() == contractAddress.lower())
            if abi_info is not None:
                for abi_detail in abi_info['abi']:
                    if abi_detail['type'] in ['constructor', 'fallback', 'receive']:
                        continue
                    # abi_info_input
                    function_name = '{name}({params})'.format(name=abi_detail['name'],
                                                              params=','.join(
                                                                  pydash.map_(abi_detail['inputs'], 'type')))
                    insert_data = {
                        'contract_address': contractAddress,  # 合约地址
                        'project': project,  # 项目名
                        'type': abi_detail['type'],  # abi 类型
                        'name': abi_detail['name'],  # 名称
                        'function_name': function_name,
                        'parametric_name': '{name}({params})'.format(name=abi_detail['name'], params=','.join(
                            pydash.map_(abi_detail['inputs'], lambda input: input['type'] + ':' + input['name']))),
                        'sha3': Web3().sha3(text=function_name).hex()
                    }

                    exists = AbiInfoInput.find_one(
                        {'contract_address': contractAddress, 'name': abi_detail['name'], 'type': abi_detail['type']})
                    if exists is None:
                        res = AbiInfoInput.insert_one(insert_data)
