from utils.query_bigquery import query_bigquery
import moment
from datetime import timedelta, datetime
from models.defi_up_contract import get_model_instance

class BlockNumberDaily:

    def insert_to_mongo(self, data):
        model = get_model_instance('date_block_daily')
        for item in data:
            model.find_one_and_update({'blockTime': item['last_block_timestamp'],
                                                      'blockTimeStr': datetime.combine(item['timestamp'],
                                                                                  datetime.min.time()).strftime(
                                                          "%Y-%m-%d"),
                                                      'chain': item['chain']},
                                                     {'blockNumber': item['last_block_number']}, True)

    def query_by_day(self, start_date=None, end_date=None):
        filters = []
        if start_date is not None:
            filters.append(f"DATE(block_timestamp) >= '{start_date}'")
        if end_date is not None:
            filters.append(f"DATE(block_timestamp) < '{end_date}'")
        match_date_filter = ' AND '.join(filters)
        sql = """
        SELECT DATE(block_timestamp) as timestamp,MAX(block_timestamp) AS last_block_timestamp,MAX(block_number) AS last_block_number,'ethereum' AS chain FROM `footprint-blockchain-etl.crypto_ethereum.transactions` 
        WHERE {match_date_filter} group by DATE(block_timestamp)
        UNION ALL 
        SELECT DATE(block_timestamp) as timestamp,MAX(block_timestamp) AS last_block_timestamp,MAX(block_number) AS last_block_number,'bsc' AS chain FROM `footprint-blockchain-etl.crypto_bsc.transactions` 
        WHERE {match_date_filter} group by DATE(block_timestamp)
        UNION ALL 
        SELECT DATE(block_timestamp) as timestamp,MAX(block_timestamp) AS last_block_timestamp,MAX(block_number) AS last_block_number,'polygon' AS chain FROM `footprint-blockchain-etl.crypto_polygon.transactions` 
        WHERE {match_date_filter} group by DATE(block_timestamp)
        """.format(match_date_filter=match_date_filter)
        result = query_bigquery(sql)
        return result.to_dict('records')

    def run_daily_job(self):
        # 因为是幂等，更新最近5天的数据
        now = moment.utcnow().datetime
        start_date = (now - timedelta(days=5)).strftime("%Y-%m-%d")  # 获得前5天的时间
        end_date = (now - timedelta(days=1)).strftime("%Y-%m-%d")  # 获得前1天的时间
        data = self.query_by_day(start_date, end_date)
        self.insert_to_mongo(data)

    def parse_history_data(self):
        now = moment.utcnow().datetime
        end_date = (now - timedelta(days=1)).strftime("%Y-%m-%d")  # 获得前1天的时间
        data = self.query_by_day(None, end_date)
        print(f'共{len(data)}条数据')
        self.insert_to_mongo(data)


if __name__ == '__main__':
    ins = BlockNumberDaily()
    ins.parse_history_data()
