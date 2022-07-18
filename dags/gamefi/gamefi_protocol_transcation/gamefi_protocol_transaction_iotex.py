from gamefi.gamefi_protocol_transcation.gamefi_protocol_transaction_polygon import GamefiProtocolTransactionPolygon
from utils.query_bigquery import query_bigquery


class GamefiProtocolTransactionIoTeX(GamefiProtocolTransactionPolygon):
    task_airflow_execution_time = '0 1 * * *'
    task_name = 'gamefi_protocol_transaction_iotex'
    table_name = 'gamefi_protocol_transaction_iotex'
    chain = 'IoTeX'
    data = []

    def exec(self):
        sql = f"""
            INSERT INTO
              `xed-project-237404.footprint_etl.{self.table_name}` (protocol_id,
                chain,
                protocol_name,
                protocol_slug,
                contract_address,
                wallet_address,
                block_timestamp,
                method_id,
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
              data.action_type AS method_id,
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
              AND data.action_type = 'execution'
              AND DATE(tx.block_timestamp) = '{self.execution_date_str}'
                """
        self.data = query_bigquery(sql)


if __name__ == '__main__':
    GamefiProtocolTransactionIoTeX().exec()
    print('run success')
