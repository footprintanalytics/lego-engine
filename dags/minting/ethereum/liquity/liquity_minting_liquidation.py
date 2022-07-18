from minting.ethereum.minting_model import MintingLiquidationModel


class LiquityMintingLiquidation(MintingLiquidationModel):
    history_date = '2021-12-12'
    project_name = 'Liquity'
    task_name = 'liquity_minting_liquidation'
    execution_time = '5 3 * * *'
    source_event_sql_file = 'minting/ethereum/liquity/liquity_minting_liquidation.sql'
    dataset_name_prefix = 'liquity'


if __name__ == '__main__':
    pool = LiquityMintingLiquidation()

    # 同项目目录生成完整sql
    daily_sql = pool.build_daily_data_sql()
    file1 = open('daily_sql_liquidation.sql', 'w')
    file1.write(daily_sql)

    # 跑一天的数据
    # pool.run_daily_job()

    # 跑历史数据
    # pool.parse_history_data()

    # 合历史和日增表为总的视图
    # pool.create_all_data_view()