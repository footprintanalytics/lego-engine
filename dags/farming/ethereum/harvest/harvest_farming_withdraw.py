from farming.ethereum.farming_model import FarmingModel


class HarvestFarmingWithdraw(FarmingModel):
    history_date = '2021-12-15'
    project_name = 'Harvest'
    task_name = 'harvest_farming_withdraw'
    execution_time = '5 3 * * *'
    source_event_sql_file = 'farming/ethereum/harvest/harvest_farming_withdraw.sql'
    dataset_name_prefix = 'harvest'


if __name__ == '__main__':
    pool = HarvestFarmingWithdraw()

    # 同项目目录生成完整sql
    # daily_sql = pool.build_daily_data_sql()
    # file1 = open('daily_sql.sql', 'w')
    # file1.write(daily_sql)

    # 跑一天的数据
    # pool.run_daily_job()

    # 跑历史数据
    # pool.parse_history_data()

    # 合历史和日增表为总的视图
    # pool.create_all_data_view()

    pool.validate()
