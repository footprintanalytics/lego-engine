from basic.etl_basic import ETLBasic
from utils.date_util import DateUtil
from utils.query_bigquery import query_bigquery

class GamefiProtocolTransactionPolygon(ETLBasic):
    task_airflow_execution_time = '30 3 * * *'
    task_name = 'gamefi_protocol_transaction_polygon'
    table_name = 'gamefi_protocol_transaction_polygon'
    chain = 'Polygon'
    task_category = 'gamefi'
    data = []

    @property
    def origin_table_name(self):
        origin_table = f'gaia-data.struct_data.{self.chain.lower()}_transactions'
        return origin_table

    def get_execution_date(self):
        return DateUtil.utc_start_of_date(DateUtil.utc_24h_ago())

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
              SUBSTR(data.input, 1, 10) AS method_id,
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

    def airflow_steps(self):
        return [
            self.exec,
            self.do_task_execution_flag
        ]


if __name__ == '__main__':
    GamefiProtocolTransactionPolygon().exec()
    print('run success')
