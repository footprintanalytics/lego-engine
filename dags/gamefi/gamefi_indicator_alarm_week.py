from datetime import datetime

import pydash

from alarm.rise_fall_7d_change_alarm_gamefi_active_users import RiseFall7DChangeAlarmGamefiActiveUsers
from alarm.rise_fall_7d_change_alarm_gamefi_new_users import RiseFall7DChangeAlarmGamefiNewUsers
from utils.date_util import DateUtil
from utils.query_bigquery import query_bigquery

class GamefiIndicatorAlarmWeek:

    def __init__(self, reference_date: datetime = None):
        if not reference_date:
            reference_date = self.get_reference_date()
        self.reference_date = reference_date

    def exec(self):
        last_day_data = self.get_last_day_data()
        print(last_day_data)
        if not pydash.get(last_day_data, '0.count'):
            print('没有昨天数据')
            return
        self.do_alarm()

    def get_last_day_data(self):
        sql = """
            select count(0) as count from `gaia-data.origin_data.protocol_daily_stats` 
            where day = '{day}'
        """.format(day=self.reference_date.strftime('%Y-%m-%d'))
        result = query_bigquery(sql)
        return result.to_dict('records')

    def do_alarm(self):
        RiseFall7DChangeAlarmGamefiActiveUsers(self.reference_date).alarm()
        RiseFall7DChangeAlarmGamefiNewUsers(self.reference_date).alarm()

    def get_reference_date(self):
        return DateUtil.utc_start_of_date(DateUtil.utc_x_hours_ago(24))

if __name__ == '__main__':
    GamefiIndicatorAlarmWeek().exec()