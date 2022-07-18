from models import ETHBridgesTvlOverTime
import pydash, pandas
from datetime import datetime
from utils.date_util import DateUtil
from config import project_config
from bridges.bridges_base import Bridges

project_name = 'eth_bridges_tvl_over_time_daily'
query_id = 'query_id'

colums = [
    'day',
    'bridge',
    'tvl_usd'
]

class EthBridgesTvlOverTime(Bridges):
    def __init__(self):
        super(EthBridgesTvlOverTime, self).__init__(query_id=query_id, dags_folder=project_config.dags_folder, project_name=project_name)

    def save_data(self):
        data = self.get_data()
        get_result_by_result_id:list = data['data']['get_result_by_result_id']
        for item in get_result_by_result_id:
            info = item['data']
            bridge = pydash.get(info, 'bridge')
            day = DateUtil.utc_start_of_date(datetime.fromisoformat(pydash.get(info, 'day')))
            query = {
                "bridge": bridge,
                "day": day
            }
            update = {
                "bridge": bridge,
                "day": day,
                "tvl_usd": pydash.get(info, 'tvl_usd'),
                'created_at': DateUtil.utc_current(),
                'updated_at': DateUtil.utc_current()
            }
            ETHBridgesTvlOverTime.update_one(query=query, set_dict=update, upsert=True)
            print(f'bridge: {bridge}, day: {day}')

    def handle_write_data_to_csv(self):
        query = {}
        tvl_info = ETHBridgesTvlOverTime.find(query)
        values_list = []
        for data in tvl_info:
            day = pydash.get(data, 'day')
            tvl_usd = pydash.get(data, 'tvl_usd')
            bridge = pydash.get(data, 'bridge')
            value = {
                "day": day,
                "tvl_usd": tvl_usd,
                "bridge": bridge
            }
            values_list.append(value)
        df = pandas.DataFrame(data=values_list, columns=colums)
        csv_file_name = self.get_stats_csv_name()
        df.to_csv(csv_file_name, index=False, header=True)
        self.handle_upload_csv_to_gsc()

if __name__ == '__main__':
    a = EthBridgesTvlOverTime()
    a.save_data()
    a.handle_write_data_to_csv()
    a.handle_import_gsc_csv_to_bigquery()