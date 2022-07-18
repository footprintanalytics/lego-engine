from datetime import datetime, timedelta

import pandas as pd
import pytz


class DateUtil(object):
    IND_TIMEZONE = pytz.timezone('Asia/Kolkata')
    ZH_TIMEZONE = pytz.timezone('Asia/Shanghai')
    UTC_TIMEZONE = pytz.timezone('UTC')

    """
    创建datetime的时候
    ex: 
        datetime(2019, 9, 27, 14, 7, tzinfo=DateUtil.IND_TIMEZONE)
        User.find_list({
            'g': {
                '$gt': datetime(2019, 9, 27, 14, 7, tzinfo=DateUtil.IND_TIMEZONE) - timedelta(hours=1),
                '$lt': datetime(2019, 9, 27, 14, 7, tzinfo=DateUtil.IND_TIMEZONE) + timedelta(hours=1)
            }
        })
    """

    @classmethod
    def ind_current(cls) -> datetime:
        """
        获取印度的当前时间, 用于insert数据到db
        ex:
            User.insert_one({'mobileNumber': '1233212293', 'createdAt': DateUtil.ind_current()})
        """

        return datetime.now(cls.IND_TIMEZONE)

    @classmethod
    def zh_current(cls) -> datetime:
        """
        获取中国的当前时间
        """
        return datetime.now(cls.ZH_TIMEZONE)

    @classmethod
    def utc_current(cls) -> datetime:
        """
        获取UTC当前时间, for 校验 ind_current 暂时没有想到用处
        """
        return datetime.now(cls.UTC_TIMEZONE)

    @classmethod
    def utc_start_of_date(cls, dt: datetime = None) -> datetime:
        """
        获取UTC当日开始时间
        """
        if not dt:
            dt = cls.utc_current()
        return cls.UTC_TIMEZONE.localize(datetime(dt.year, dt.month, dt.day))

    @classmethod
    def utc_end_of_date(cls, dt: datetime = None) -> datetime:
        """
        获取UTC当日结束时间
        """
        if not dt:
            dt = cls.utc_current()
        zero_time = cls.utc_start_of_date(dt)
        last_time = zero_time + timedelta(hours=23, minutes=59, seconds=59, milliseconds=999, microseconds=999)
        return last_time

    @classmethod
    def get_date(cls) -> datetime:
        """
        获取UTC24小时前时间
        """
        return datetime.now(cls.UTC_TIMEZONE) - timedelta(days=1)

    @classmethod
    def utc_24h_ago(cls) -> datetime:
        """
        获取UTC24小时前时间
        """
        return datetime.now(cls.UTC_TIMEZONE) - timedelta(days=1)

    @classmethod
    def utc_x_hours_ago(cls, x: int, dt: datetime = None) -> datetime:
        """
       获取UTC x 小时前时间
       """
        if not dt:
            dt = cls.utc_current()
        return dt - timedelta(hours=x)

    @classmethod
    def utc_x_hours_after(cls, x: int, dt: datetime = None) -> datetime:
        """
       获取UTC x小时后时间
       """
        if not dt:
            dt = cls.utc_current()
        return dt + timedelta(hours=x)

    @classmethod
    def utc_x_minutes_after(cls, x: int, dt: datetime = None) -> datetime:
        """
       获取UTC x分钟后时间
       """
        if not dt:
            dt = cls.utc_current()
        return dt + timedelta(minutes=x)

    @classmethod
    def utc_to_ind(cls, dt: datetime) -> datetime:
        """
        utc datetime to 印度datetime , db find 出来的转换
        ex:
            user = User.find_one({'createdAt': {'$exists': True}}, {'_id': 0, 'createdAt': 1})
            createdAt = DateUtil.utc_to_ind(user['createdAt'])
        """
        if dt.tzinfo is None:
            dt = cls.UTC_TIMEZONE.localize(dt)
        return dt.astimezone(cls.IND_TIMEZONE)

    @classmethod
    def ind_to_utc(cls, dt: datetime) -> datetime:
        """
        印度 timezone 的 datetime to UTC datetime, for 校验 ind_to_utc 暂时没有想到用处
        """
        if dt.tzinfo is None:
            dt = cls.IND_TIMEZONE.localize(dt)
        return dt.astimezone(cls.UTC_TIMEZONE)

    @classmethod
    def zh_to_ind(cls, dt: datetime) -> datetime:
        """
        国内 timezone 的 datetime to 印度 datetime, for 转换数据
        """
        if dt.tzinfo is None:
            dt = cls.ZH_TIMEZONE.localize(dt)
        return dt.astimezone(cls.IND_TIMEZONE)

    @classmethod
    def zh_to_utc(cls, dt: datetime) -> datetime:
        """
        国内 timezone 的 datetime to UTC datetime, for 校验 ind_to_utc 暂时没有想到用处
        """
        if dt.tzinfo is None:
            dt = cls.ZH_TIMEZONE.localize(dt)
        return dt.astimezone(cls.UTC_TIMEZONE)

    @classmethod
    def ind_of_date(cls, dt: datetime):
        return cls.IND_TIMEZONE.localize(dt)

    @classmethod
    def ind_start_of_date(cls, dt: datetime = None):
        if not dt:
            dt = cls.ind_current()
        return cls.IND_TIMEZONE.localize(datetime(dt.year, dt.month, dt.day))

    @classmethod
    def ind_end_of_date(cls, dt: datetime = None):
        if not dt:
            dt = cls.ind_current()
        return cls.IND_TIMEZONE.localize(datetime(dt.year, dt.month, dt.day, 23, 59, 59, 999))

    @classmethod
    def ind_end_of_day_seconds(cls, dt: datetime = None):
        if not dt:
            dt = cls.ind_current()
        diff = cls.ind_end_of_date() - dt
        second = diff.seconds + 1 if diff.microseconds > 0 else diff.seconds  # 如果有小于1个单位的秒数, 补充一秒进去(治疗1天有86400秒的强迫症)
        return second

    @classmethod
    def zh_start_of_date(cls, dt: datetime = None):
        if not dt:
            dt = cls.zh_current()
        return cls.ZH_TIMEZONE.localize(datetime(dt.year, dt.month, dt.day))

    @classmethod
    def zh_end_of_date(cls, dt: datetime = None):
        if not dt:
            dt = cls.zh_current()
        return cls.ZH_TIMEZONE.localize(datetime(dt.year, dt.month, dt.day, 23, 59, 59, 999))

    @classmethod
    def zh_end_of_day_seconds(cls, dt: datetime = None):
        if not dt:
            dt = cls.zh_current()
        diff = cls.zh_end_of_date() - dt
        second = diff.seconds + 1 if diff.microseconds > 0 else diff.seconds  # 如果有小于1个单位的秒数, 补充一秒进去(治疗1天有86400秒的强迫症)
        return second

    @classmethod
    def days_diff(cls, start: datetime, end: datetime):
        st = cls.ind_start_of_date(cls.utc_to_ind(start))
        et = cls.ind_start_of_date(cls.utc_to_ind(end))
        return (et - st).days

    # @classmethod
    # def diff(cls, start: datetime, end: datetime):
    #




    @classmethod
    def tz_localize(cls, dt_sr: pd.Series, tz: str) -> pd.Series:
        return dt_sr.dt.tz_localize(tz)
