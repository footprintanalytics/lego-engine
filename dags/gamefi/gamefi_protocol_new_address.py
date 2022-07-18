from basic.etl_basic import ETLBasic
from utils.query_bigquery import query_bigquery

class GameFiProtocolNewAddress(ETLBasic):
    task_airflow_execution_time = '50 3 * * *'
    task_name = 'gamefi_protocol_new_address'
    table_name = 'gamefi_protocol_new_address'
    task_category = 'gamefi'
    data = []

    def exec(self):
        sql = f"""
            INSERT INTO
              `xed-project-237404.footprint_etl.gamefi_protocol_new_address` (first_day,
                protocol_name,
                protocol_slug,
                chain,
                wallet_address,
                type )
            SELECT
              first_day,
              protocol_name,
              protocol_slug,
              chain,
              wallet_address,
              'GameFi' AS type
            FROM (
              SELECT
                chain,
                protocol_slug,
                protocol_name,
                wallet_address,
                MIN(DATE(block_timestamp)) AS first_day
              FROM
                `xed-project-237404.footprint.gamefi_protocol_transaction`
              GROUP BY
                chain,
                protocol_slug,
                protocol_name,
                wallet_address )dnu
            WHERE
              NOT EXISTS (
              SELECT
                1
              FROM
                `xed-project-237404.footprint_etl.gamefi_protocol_new_address` nu
              WHERE
                dnu.chain = nu.chain
                AND dnu.protocol_slug = nu.protocol_slug
                AND dnu.protocol_name = nu.protocol_name
                AND dnu.first_day = nu.first_day
                AND dnu.wallet_address = nu.wallet_address )
                """
        self.data = query_bigquery(sql)

    def airflow_steps(self):
        return [
            self.exec,
            self.do_task_execution_flag
        ]


if __name__ == '__main__':
    GameFiProtocolNewAddress().exec()
    print('run success')
