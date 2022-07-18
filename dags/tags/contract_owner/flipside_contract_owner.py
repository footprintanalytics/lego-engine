from models import PoolInfo, FlipsideAddressLabel
from tags.etl_to_tags import EtlToTags


class FlipSideContractOwner(EtlToTags):

    def origin_data(self):
        project_ls = PoolInfo.distinct('project')
        return FlipsideAddressLabel.find_list({'label': {'$in': project_ls}}, {'label': 1, 'address': 1, '_id': 0})

    def etl(self, origin_data) -> {'tag_name': str, 'entity_type_ns_name': str, 'entity_ns_name': str,
                                   'entity_id': str}:

        return {
            'tag_name': origin_data['label'].lower(),
            'entity_type_ns_name': 'contract_owner',
            'entity_ns_name': 'ethereum_chain/third_resources/flipside',
            'entity_id': origin_data['address'].lower()
        }

if __name__ == '__main__':
    etl = FlipSideContractOwner()
    etl.process()
    print('done')