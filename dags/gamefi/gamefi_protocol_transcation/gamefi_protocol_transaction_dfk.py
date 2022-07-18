from gamefi.gamefi_protocol_transcation.gamefi_protocol_transaction_polygon import GamefiProtocolTransactionPolygon


class GamefiProtocolTransactionDFK(GamefiProtocolTransactionPolygon):
    task_airflow_execution_time = '0 1 * * *'
    task_name = 'gamefi_protocol_transaction_dfk'
    table_name = 'gamefi_protocol_transaction_dfk'
    chain = 'DFK'


if __name__ == '__main__':
    GamefiProtocolTransactionDFK().exec()
    print('run success')


