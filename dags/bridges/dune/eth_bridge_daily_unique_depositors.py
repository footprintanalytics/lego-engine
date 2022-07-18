from models import ETHBridgesDailyUniqueDepositors
import pydash, pandas
from datetime import datetime
from utils.date_util import DateUtil
from config import project_config
from bridges.bridges_base import Bridges

project_name = 'eth_bridges_daily_unique_depositors_daily'
query_id = 'query_id'

colums = [
    'day',
    'bridge',
    'unique_depositors'
]

class EthBridgesDailyUniqueDepositors(Bridges):
    def __init__(self):
        super(EthBridgesDailyUniqueDepositors, self).__init__(query_id=query_id, dags_folder=project_config.dags_folder, project_name=project_name)

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
                "unique_depositors": pydash.get(info, 'unique_depositors'),
                'created_at': DateUtil.utc_current(),
                'updated_at': DateUtil.utc_current()
            }
            ETHBridgesDailyUniqueDepositors.update_one(query=query, set_dict=update, upsert=True)
            print(f'bridge: {bridge}, day: {day}')

    def handle_write_data_to_csv(self):
        query = {}
        tvl_info = ETHBridgesDailyUniqueDepositors.find(query)
        values_list = []
        for data in tvl_info:
            day = pydash.get(data, 'day')
            unique_depositors = pydash.get(data, 'unique_depositors')
            bridge = pydash.get(data, 'bridge')
            value = {
                "day": day,
                "bridge": bridge,
                "unique_depositors": unique_depositors

            }
            values_list.append(value)
        df = pandas.DataFrame(data=values_list, columns=colums)
        csv_file_name = self.get_stats_csv_name()
        df.to_csv(csv_file_name, index=False, header=True)
        self.handle_upload_csv_to_gsc()

if __name__ == '__main__':
    a = EthBridgesDailyUniqueDepositors()
    a.save_data()
    a.handle_write_data_to_csv()
    a.handle_import_gsc_csv_to_bigquery()