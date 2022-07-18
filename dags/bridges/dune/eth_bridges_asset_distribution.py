from models import ETHBridgesAssetDistribution
import pydash, pandas
from datetime import datetime
from utils.date_util import DateUtil
from config import project_config
from bridges.bridges_base import Bridges

project_name = 'eth_bridges_asset_distribution_daily'
query_id = 'query_id'

colums = [
    'label',
    'ranking',
    'volume_all_share'
]

class EthBridgesAssetDistribution(Bridges):
    def __init__(self):
        super(EthBridgesAssetDistribution, self).__init__(query_id=query_id, dags_folder=project_config.dags_folder, project_name=project_name)

    def save_data(self):
        data = self.get_data()
        get_result_by_result_id:list = data['data']['get_result_by_result_id']
        for item in get_result_by_result_id:
            info = item['data']
            label = pydash.get(info, 'label')
            query = {
                "label": label,
            }
            update = {
                "label": label,
                "ranking": pydash.get(info, 'ranking'),
                "volume_all_share": pydash.get(info, 'tvl_usd'),
                'created_at': DateUtil.utc_current(),
                'updated_at': DateUtil.utc_current()
            }
            ETHBridgesAssetDistribution.update_one(query=query, set_dict=update, upsert=True)
            print(f'label: {label}')

    def handle_write_data_to_csv(self):
        query = {}
        tvl_info = ETHBridgesAssetDistribution.find(query)
        values_list = []
        for data in tvl_info:
            label = pydash.get(data, 'label')
            ranking = pydash.get(data, 'ranking')
            volume_all_share = pydash.get(data, 'volume_all_share')
            value = {
                "label": label,
                "ranking": ranking,
                "volume_all_share": volume_all_share
            }
            values_list.append(value)
        df = pandas.DataFrame(data=values_list, columns=colums)
        csv_file_name = self.get_stats_csv_name()
        df.to_csv(csv_file_name, index=False, header=True)
        self.handle_upload_csv_to_gsc()

if __name__ == '__main__':
    a = EthBridgesAssetDistribution()
    a.save_data()
    a.handle_write_data_to_csv()
    a.handle_import_gsc_csv_to_bigquery()