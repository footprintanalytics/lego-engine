from gamefi.gamefi_protocol_transcation.gamefi_protocol_transaction_polygon import GamefiProtocolTransactionPolygon


class GamefiProtocolTransactionCelo(GamefiProtocolTransactionPolygon):
    task_airflow_execution_time = '0 1 * * *'
    task_name = 'gamefi_protocol_transaction_celo'
    table_name = 'gamefi_protocol_transaction_celo'
    chain = 'Celo'


if __name__ == '__main__':
    GamefiProtocolTransactionCelo().exec()
    print('run success')


