from lending.bsc.lending_model_bsc import LendingModelBsc


class VenusLendingLiquidation(LendingModelBsc):
    history_date = '2021-12-19'
    project_name = 'bsc_venus'
    task_name = 'bsc_venus_lending_liquidation'
    source_event_sql_file = 'lending/bsc/venus/venus_lending_liquidation.sql'
    dataset_name_prefix = 'venus'


if __name__ == '__main__':
    pool = VenusLendingLiquidation()
    # pool.run_daily_job()
    # pool.parse_history_data()
    # pool.create_all_data_view()
    pool.validate()
    # 同项目目录生成完整sql
    # daily_sql = pool.build_daily_data_sql()
    # file1 = open('daily_sql_liquidation.sql', 'w')
    # file1.write(daily_sql)

    # 跑一天的数据
    # pool.run_daily_job()

    # 跑历史数据
    # pool.parse_history_data()

    # 合历史和日增表为总的视图
    # pool.create_all_data_view()