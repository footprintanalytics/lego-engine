from farming.bsc.farming_model import FarmingModel


class MdexFarmingReward(FarmingModel):
    history_date = '2021-12-16'
    project_name = 'Mdex'
    task_name = 'mdex_farming_reward'
    execution_time = '5 3 * * *'
    source_event_sql_file = 'farming/bsc/mdex/mdex_farming_reward.sql'
    dataset_name_prefix = 'mdex'
