from lending.ethereum.lending_model import LendingModel


class CompoundLendingCollateralChange(LendingModel):
    history_date = '2021-12-09'
    project_name = 'Compound'
    task_name = 'compound_lending_collateral_change'
    execution_time = '5 3 * * *'
    source_event_sql_file = 'lending/ethereum/compound/compound_lending_collateral_change.sql'
    dataset_name_prefix = 'compound'