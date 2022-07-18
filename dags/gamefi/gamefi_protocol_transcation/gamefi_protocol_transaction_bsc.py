from gamefi.gamefi_protocol_transcation.gamefi_protocol_transaction_polygon import GamefiProtocolTransactionPolygon
from utils.query_bigquery import query_bigquery


class GamefiProtocolTransactionBsc(GamefiProtocolTransactionPolygon):
    task_airflow_execution_time = '30 3 * * *'
    task_name = 'gamefi_protocol_transaction_bsc'
    table_name = 'gamefi_protocol_transaction_bsc'
    chain = 'BSC'


    def exec(self):
        # note by Pen. 这个表里面多了 day 和method字段, 修改需要重刷表
        sql = f"""
            INSERT INTO
              `xed-project-237404.footprint_etl.{self.table_name}` ( protocol_id,
                chain,
                protocol_name,
                protocol_slug,
                contract_address,
                wallet_address,
                block_timestamp,
                day,
                method_id,
                method_name,
                tx_hash,
                value)
            SELECT
              info.protocol_id,
              info.chain,
              info.protocol_name,
              info.protocol_slug,
              info.contract_address AS contract_address,
              tx.from_address AS wallet_address,
              tx.block_timestamp AS block_timestamp,
              tx.block_timestamp AS day,
              SUBSTR(data.input, 1, 10) AS method_id,
              SUBSTR(data.input, 1, 10) AS method_name,
              tx.tx_hash AS tx_hash,
              tx.value AS value
            FROM
              `xed-project-237404.footprint_etl.gamefi_contract_info` info
            LEFT JOIN
              `{self.origin_table_name}` tx
            ON
              LOWER(info.contract_address) = tx.to_address
            WHERE
              NOT EXISTS (
              SELECT
                 1
              FROM
                `xed-project-237404.footprint_etl.{self.table_name}` n_tx
              WHERE
                tx.tx_hash = n_tx.tx_hash
                AND tx.block_timestamp = n_tx.block_timestamp )
              AND info.chain = '{self.chain}'
              AND tx.status = 'success'
              AND SUBSTR(data.input, 1, 10) != '0x095ea7b3'
              AND SUBSTR(data.input, 1, 10) != '0xa9059cbb'
              AND DATE(tx.block_timestamp) = '{self.execution_date_str}'
                """
        self.data = query_bigquery(sql)

if __name__ == '__main__':
    GamefiProtocolTransactionBsc().exec()
    print('run success')
