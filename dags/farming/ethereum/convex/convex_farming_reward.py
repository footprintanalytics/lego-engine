from farming.ethereum.farming_model import FarmingModel


class ConvexFarmingReward(FarmingModel):
    history_date = '2021-12-15'
    project_name = 'Convex'
    task_name = 'convex_farming_reward'
    execution_time = '5 3 * * *'
    source_event_sql_file = 'farming/ethereum/convex/convex_farming_reward.sql'
    dataset_name_prefix = 'convex'
