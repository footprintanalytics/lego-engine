from models import PoolInfo, EtherscanAddressLabel
from tags.etl_to_tags import EtlToTags


class EtherScanContractOwner(EtlToTags):

    def origin_data(self):
        project_ls = PoolInfo.distinct('project')
        return EtherscanAddressLabel.find_list({'label': {'$in': project_ls}} , {'label': 1, 'address': 1, '_id': 0})

    def etl(self, origin_data):
        return {
            'tag_name': origin_data['label'].lower(),
            'entity_type_ns_name': 'contract_owner',
            'entity_ns_name': 'ethereum_chain/third_resources/etherscan',
            'entity_id': origin_data['address'].lower()
        }

if __name__ == '__main__':
    etherScan = EtherScanContractOwner()
    etherScan.process()
    print('done')