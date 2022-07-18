from farming.ethereum.farming_model import FarmingModel


class SushiFarmingWithdraw(FarmingModel):
    history_date = '2021-12-13'
    project_name = 'Sushi'
    task_name = 'sushi_farming_withdraw'
    execution_time = '5 3 * * *'
    source_event_sql_file = 'farming/ethereum/sushi/sushi_farming_withdraw.sql'
    dataset_name_prefix = 'sushi'
