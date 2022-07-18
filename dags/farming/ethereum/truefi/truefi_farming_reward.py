from farming.ethereum.farming_reward_model import FarmingModel


class TruefiFarmingReward(FarmingModel):
    history_date = '2021-12-16'
    project_name = 'Truefi'
    task_name = 'truefi_farming_reward'
    execution_time = '5 3 * * *'
    source_event_sql_file = 'farming/ethereum/truefi/truefi_farming_reward.sql'
    dataset_name_prefix = 'truefi'


if __name__ == '__main__':
    pool = TruefiFarmingReward()

    # 同项目目录生成完整sql
    daily_sql = pool.build_daily_data_sql()
    file1 = open('daily_sql.sql', 'w')
    file1.write(daily_sql)

    # 跑一天的数据
    pool.run_daily_job()

    # 跑历史数据
    pool.parse_history_data()

    # 合历史和日增表为总的视图
    pool.create_all_data_view()

    # 基础校验
    # pool.validate()