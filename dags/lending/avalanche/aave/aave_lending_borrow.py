from lending.avalanche.lending_model_avalanche import LendingModelAvalanche


class AaveLendingBorrow(LendingModelAvalanche):
    history_date = '2021-12-21'
    project_name = 'avalanche_aave'
    task_name = 'avalanche_aave_lending_borrow'
    execution_time = '5 3 * * *'
    source_event_sql_file = 'lending/avalanche/aave/aave_lending_borrow.sql'
    dataset_name_prefix = 'aave'


if __name__ == '__main__':
    pool = AaveLendingBorrow()

    # 同项目目录生成完整sql
    # daily_sql = pool.build_daily_data_sql()
    # file1 = open('daily_sql_borrow.sql', 'w')
    # file1.write(daily_sql)

    # 跑一天的数据
    pool.run_daily_job()

    # 跑历史数据
    pool.parse_history_data()

    # 合历史和日增表为总的视图
    # pool.create_all_data_view()