from gamefi.gamefi_protocol_transcation.gamefi_protocol_transaction_polygon import GamefiProtocolTransactionPolygon


class GamefiProtocolTransactionFantom(GamefiProtocolTransactionPolygon):
    task_airflow_execution_time = '0 1 * * *'
    task_name = 'gamefi_protocol_transaction_fantom'
    table_name = 'gamefi_protocol_transaction_fantom'
    chain = 'Fantom'
    data = []


if __name__ == '__main__':
    GamefiProtocolTransactionFantom().exec()
    print('run success')
