from minting.ethereum.minting_model import MintingWithdrawModel


class LiquityMintingWithdraw(MintingWithdrawModel):
    history_date = '2021-12-12'
    project_name = 'Liquity'
    task_name = 'liquity_minting_withdraw'
    execution_time = '5 3 * * *'
    source_event_sql_file = 'minting/ethereum/liquity/liquity_minting_withdraw.sql'
    dataset_name_prefix = 'liquity'


if __name__ == '__main__':
    pool = LiquityMintingWithdraw()

    # 同项目目录生成完整sql
    # daily_sql = pool.build_daily_data_sql()
    # file1 = open('daily_sql_withdraw.sql', 'w')
    # file1.write(daily_sql)

    # 跑一天的数据
    pool.validate()

    # 跑历史数据
    # pool.parse_history_data()

    # 合历史和日增表为总的视图
    # pool.create_all_data_view()