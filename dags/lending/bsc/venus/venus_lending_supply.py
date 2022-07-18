from lending.bsc.lending_model_bsc import LendingModelBsc


class VenusLendingSupply(LendingModelBsc):
    project_name = 'bsc_venus'
    task_name = 'bsc_venus_lending_supply'
    source_event_sql_file = 'lending/bsc/venus/venus_lending_supply.sql'
    dataset_name_prefix = 'venus'
    history_date = '2021-12-19'


if __name__ == '__main__':
    lending = VenusLendingSupply()
    lending.parse_history_data()
    # lending.run_daily_job()
    lending.create_all_data_view()
    # daily_sql = lending.build_daily_data_sql()
    # file1 = open('daily_mint_sql.sql', 'w')
    # file1.write(daily_sql)
    #
    # history_sql = lending.build_history_data_sql()
    # file2 = open('history_mint_sql.sql', 'w')
    # file2.write(history_sql)