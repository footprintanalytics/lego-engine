from lending.ethereum.lending_model import LendingModel


class AaveLendingCollateralChange(LendingModel):
    history_date = '2021-12-09'
    project_name = 'Aave'
    task_name = 'aave_lending_collateral_change'
    execution_time = '5 3 * * *'
    source_event_sql_file = 'lending/ethereum/aave/aave_lending_collateral_change.sql'
    dataset_name_prefix = 'aave'
