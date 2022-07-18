import pandas as pd
from defi360.common.python_adapter import PythonAdapter
import operator as op

class MonthlyProtocolAddressRetention(PythonAdapter):
    # 自己确定数据集
    project_id = 'xed-project-237404'
    data_set = 'footprint_etl'
    task_name = "monthly_protocol_address_retention"
    execution_time = "10 4 * * *"
    schema_name = "defi360/schema/monthly_protocol_address_retention.json"
    sql_path = "defi360/python_adapter/monthly_protocol_address_retention.sql"


    def python_etl(self, df: pd.DataFrame):
        df = df[['protocol_slug', 'wallet_address','chain', 'day']].drop_duplicates()
        df['day'] =pd.to_datetime(df['day'])
        df = df.assign(acquisition_cohort=df.groupby(['wallet_address','chain','protocol_slug'])['day'].transform('min').dt.to_period("M"))
        df = df.assign(order_cohort=df['day'].dt.to_period("M"))
        df = df.groupby(['acquisition_cohort', 'order_cohort', 'protocol_slug','chain']).agg(wallet_address=('wallet_address', 'nunique')).reset_index(drop=False)
        df['periods'] = ((df.order_cohort - df.acquisition_cohort).apply(op.attrgetter('n')))
        df = df.pivot_table(index=['protocol_slug','chain','acquisition_cohort'],
                                                   columns='periods',
                                                      values='wallet_address')

        af = df.loc[:,[0]]
        af.columns = ['total']
        df = df.divide(df.iloc[:, 0], axis=0).round(4)
        df = af.merge(df, on=['protocol_slug','chain', 'acquisition_cohort'])
        columns_name1 = df.columns.values.tolist()
        columns_name = []
        columns_name.clear()
        for i in columns_name1:
            if i == 'total':
                columns_name.append(i)
            else:
                columns_name.append('m' + str(i))
        df.columns = columns_name
        df = df.reset_index()
        df['acquisition_cohort'] = df['acquisition_cohort'].astype(str)
        return df

if __name__ == '__main__':
    job = MonthlyProtocolAddressRetention()
    # # 执行 ETL
    job.run_etl(debug=False)
