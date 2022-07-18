from models import PoolInfo
from tags.etl_to_tags import EtlToTags


class DeBankIsAPool(EtlToTags):

    def origin_data(self):
        return PoolInfo.find_list({}, {'contractAddress': 1, '_id': 0})

    def etl(self, origin_data) -> {'tag_name': str, 'entity_type_ns_name': str, 'entity_ns_name': str,
                                   'entity_id': str}:

        return {
            'tag_name': 'is_a_pool',
            'entity_type_ns_name': 'is_pool',
            'entity_ns_name': 'ethereum_chain/third_resources/debank',
            'entity_id': origin_data['contractAddress'].lower()
        }

if __name__ == '__main__':
    etl = DeBankIsAPool()
    etl.process()
    print('done')