from gamefi.gamefi_protocol_transcation.gamefi_protocol_transaction_polygon import GamefiProtocolTransactionPolygon


class GamefiProtocolTransactionArbitrum(GamefiProtocolTransactionPolygon):
    task_airflow_execution_time = '30 3 * * *'
    task_name = 'gamefi_protocol_transaction_arbitrum'
    table_name = 'gamefi_protocol_transaction_arbitrum'
    chain = 'Arbitrum'


if __name__ == '__main__':
    GamefiProtocolTransactionArbitrum().exec()
    print('run success')



