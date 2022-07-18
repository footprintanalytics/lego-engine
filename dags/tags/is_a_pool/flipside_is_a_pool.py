from models import FlipsideAddressLabel, PoolInfo
from tags.etl_to_tags import EtlToTags


class FlipSideIsAPool(EtlToTags):

    def origin_data(self):
        project_ls = PoolInfo.distinct('project')
        return FlipsideAddressLabel.find_list({'label': {'$in': project_ls}, 'label_subtype': 'pool'}, {'address': 1, '_id': 0})

    def etl(self, origin_data):
        return {
            'tag_name': 'is_a_pool',
            'entity_type_ns_name': 'is_pool',
            'entity_ns_name': 'ethereum_chain/third_resources/flipside',
            'entity_id': origin_data['address'].lower()
        }

if __name__ == '__main__':
    etl = FlipSideIsAPool()
    etl.process()
    print('done')