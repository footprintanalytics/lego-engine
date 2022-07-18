from lending.ethereum.lending_model import LendingModel


class DforceLendingBorrow(LendingModel):
    history_date = '2021-12-13'
    project_name = 'Dforce'
    task_name = 'dforce_lending_borrow'
    execution_time = '5 3 * * *'
    source_event_sql_file = 'lending/ethereum/dforce/dforce_lending_borrow.sql'
    dataset_name_prefix = 'dforce'


if __name__ == '__main__':
    pool = DforceLendingBorrow()

    # 同项目目录生成完整sql
    # daily_sql = pool.build_daily_data_sql()
    # file1 = open('daily_sql_borrow.sql', 'w')
    # file1.write(daily_sql)

    # 跑一天的数据
    # pool.run_daily_job()

    # 跑历史数据
    # pool.parse_history_data()

    # 合历史和日增表为总的视图
    # pool.create_all_data_view()

    pool.validate()