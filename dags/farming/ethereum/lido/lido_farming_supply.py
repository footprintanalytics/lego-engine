from farming.ethereum.farming_model import FarmingModel


class LidoFarmingSupply(FarmingModel):
    history_date = '2021-12-17'
    project_name = 'Lido'
    task_name = 'lido_farming_supply'
    execution_time = '5 3 * * *'
    source_event_sql_file = 'farming/ethereum/lido/lido_farming_supply.sql'
    dataset_name_prefix = 'lido'


if __name__ == '__main__':
    pool = LidoFarmingSupply()

    # 同项目目录生成完整sql
    # daily_sql = pool.build_daily_data_sql()
    # file1 = open('daily_sql.sql', 'w')
    # file1.write(daily_sql)

    # 跑一天的数据
    pool.run_daily_job()

    # 跑历史数据
    pool.parse_history_data()

    # 合历史和日增表为总的视图
    pool.create_all_data_view()
