from basic.etl_basic import ETLBasic
from utils.date_util import DateUtil
from utils.monitor import send_to_slack
from utils.query_bigquery import query_bigquery


class GamefiTokenTransferBsc(ETLBasic):
    # task_airflow_execution_time = '0 4 * * *'
    task_name = 'gamefi_token_transfer_bsc_flow'
    table_name = 'gamefi_token_transfer_bsc'
    chain = 'BSC'
    task_category = 'gamefi'
    data = []

    def get_execution_date(self):
        return DateUtil.utc_start_of_date(DateUtil.utc_24h_ago())

    def exec(self):
        table = self.get_table_name()
        origin_table = self.get_origin_table_name()
        sql = f"""
                INSERT INTO
                  `xed-project-237404.footprint_etl.{table}` ( token_address,
                    from_address,
                    to_address,
                    value,
                    transaction_hash,
                    block_timestamp,
                    block_number,
                    block_hash,
                    log_index )
                SELECT
                  token_transfers.token_address,
                  token_transfers.from_address,
                  token_transfers.to_address,
                  SAFE_CAST(token_transfers.value AS FLOAT64) AS value,
                  token_transfers.transaction_hash,
                  token_transfers.block_timestamp,
                  token_transfers.block_number,
                  token_transfers.block_hash,
                  token_transfers.log_index
                FROM
                  `{origin_table}` token_transfers
                RIGHT JOIN (
                  SELECT
                    *
                  FROM
                    `xed-project-237404.footprint_etl.gamefi_contract_info`
                  WHERE chain ='BSC') contract
                ON
                  LOWER (token_transfers.token_address) = LOWER (contract.contract_address)
                WHERE
                  NOT EXISTS (
                  SELECT
                    1
                  FROM
                    `xed-project-237404.footprint_etl.{table}` n_transfer
                  WHERE
                    token_transfers.transaction_hash = n_transfer.transaction_hash
                    And n_transfer.block_timestamp = '{self.execution_date_str}'
                    AND token_transfers.log_index = n_transfer.log_index 
                    AND SAFE_CAST(token_transfers.value AS FLOAT64) = n_transfer.value 
                    AND token_transfers.token_address = n_transfer.token_address 
                    AND token_transfers.from_address = n_transfer.from_address 
                    AND token_transfers.to_address = n_transfer.to_address )
                  AND
                   DATE(token_transfers.block_timestamp) = '{self.execution_date_str}'
               """
        self.data = query_bigquery(sql)

    # 补充对应的 token 的 transfer 流水 ，注意 替换 token_address 和 时间范围
    def exec_fix_token_transfer(self):
        print("开始补数据")
        table = self.get_table_name()
        origin_table = self.get_origin_table_name()
        sql = f"""
                INSERT INTO
                  `xed-project-237404.footprint_etl.{table}` ( token_address,
                    from_address,
                    to_address,
                    value,
                    transaction_hash,
                    block_timestamp,
                    block_number,
                    block_hash,
                    log_index )
                SELECT
                  token_transfers.token_address,
                  token_transfers.from_address,
                  token_transfers.to_address,
                  SAFE_CAST(token_transfers.value AS FLOAT64) AS value,
                  token_transfers.transaction_hash,
                  token_transfers.block_timestamp,
                  token_transfers.block_number,
                  token_transfers.block_hash,
                  token_transfers.log_index
                FROM
                  `{origin_table}` token_transfers
                RIGHT JOIN (
                  SELECT
                    *
                  FROM
                    `xed-project-237404.footprint_etl.gamefi_contract_info`
                  WHERE
                    protocol_slug ='cyball') contract
                ON
                  LOWER (token_transfers.token_address) = LOWER (contract.contract_address)
                WHERE
                   DATE(token_transfers.block_timestamp) >= '2021-11-1'
                  AND
                   DATE(token_transfers.block_timestamp) < '2022-3-24'
               """
        self.data = query_bigquery(sql)


    def get_table_name(self):
        table = f'gamefi_token_transfer_{self.chain.lower()}'
        return table

    def get_origin_table_name(self):
        origin_table = f'footprint-blockchain-etl.crypto_{self.chain.lower()}.token_transfers'
        return origin_table

    def on_failure_message(self, context):
        return send_to_slack('gamefi_token_transfer_bsc 任务执行失败，影响范围: 暂无增量数据，需关注的关键维度: gamefi_token_transfer_bsc')

    def airflow_steps(self):
        return [
            # self.exec
        ]


if __name__ == '__main__':
    GamefiTokenTransferBsc().exec()
    # GamefiTokenTransferBsc().exec_fix_token_transfer()
    print('run success')
