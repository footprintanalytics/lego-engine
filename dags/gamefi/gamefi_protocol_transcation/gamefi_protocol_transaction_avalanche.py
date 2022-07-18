from gamefi.gamefi_protocol_transcation.gamefi_protocol_transaction_polygon import GamefiProtocolTransactionPolygon

class GamefiProtocolTransactionAvalanche(GamefiProtocolTransactionPolygon):
    task_airflow_execution_time = '30 3 * * *'
    task_name = 'gamefi_protocol_transaction_avalanche'
    table_name = 'gamefi_protocol_transaction_avalanche'
    chain = 'Avalanche'
    data = []
    task_category = 'gamefi'

if __name__ == '__main__':
    GamefiProtocolTransactionAvalanche().exec()
    print('run success')
