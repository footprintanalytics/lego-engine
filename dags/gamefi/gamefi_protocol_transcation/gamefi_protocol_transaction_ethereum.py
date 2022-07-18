from gamefi.gamefi_protocol_transcation.gamefi_protocol_transaction_polygon import GamefiProtocolTransactionPolygon

class GamefiProtocolTransactionEthereum(GamefiProtocolTransactionPolygon):
    task_airflow_execution_time = '0 1 * * *'
    task_name = 'gamefi_protocol_transaction_ethereum'
    table_name = 'gamefi_protocol_transaction_ethereum'
    chain = 'Ethereum'
    data = []
    task_category = 'gamefi'


if __name__ == '__main__':
    GamefiProtocolTransactionEthereum().exec()
    print('run success')
