import datetime
import pandas as pd


def get_yesterday():
    return datetime.date.today() + datetime.timedelta(days=-1)


def fill_day(origin_df: pd.DataFrame, day_column: str = 'day',
             day: str = None):
    """
    以「origin_df」的开始时间与「day」为结束时间，作为时间断填充缺失日期数据
    Parameters
    ----------
    origin_df : 被处理的DataFrame
    day_column : 关联日期的字段名
    day : 结束时间，默认为昨天
    Returns
    -------

    """
    if not day:
        day = get_yesterday()
    date_index = pd.date_range(origin_df[day_column].min(), day)
    day_df = pd.DataFrame(date_index, columns=[day_column])
    # 补全日期
    all_day = day_df.merge(origin_df, how='left', on=day_column)
    return all_day
