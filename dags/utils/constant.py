import os

PROXY_LOG_STATUS = {
    "PENDING": 'pending',
    "ERROR": "err",
    "FINISH": "finish"
}

DASH_BOARD_RULE_NAME = {
    'TASK_EXECUTION': 'task_execution',  #任务执行检查
    'FIELD_LEGAL_NULL': 'field_legal_null',  #字段合法性检查
    'FIELD_LEGAL_LESS_THAN_ZERO': 'field_legal_less_than_zero',  #字段小于0
    'FIELD_LEGAL_LESS_THAN_OR_EQ_ZERO': 'field_legal_less_than_or_eq_zero',  #字段小于0
    'FIELD_LEGAL_MORE_THAN_VALUE': 'field_legal_more_than_value',  #字段值高于某个值
    'VALID_LEGAL_ADDRESS_FORMAT': 'valid_legal_address_format',  #地址字段不合法
    'FIELD_ANOMALY': 'field_anomaly',  #字段波动性检查
    'FIELD_CONTINUITY': 'data_continuity',  # 数据连续性检查
    'POOL_TOKEN_BALANCE': 'pool_token_balance'  # pool token_balance检查
}
DASH_BOARD_RULE_NAME_DESC_CN = {
    'TASK_EXECUTION': '任务有效性',
    'FIELD_LEGAL_NULL': '数据NULL验证',
    'FIELD_LEGAL_LESS_THAN_ZERO': '数据小于0验证',
    'FIELD_LEGAL_LESS_THAN_OR_EQ_ZERO': '数据小于等于0验证',
    'FIELD_LEGAL_MORE_THAN_VALUE': '数据大于某个阀值验证',
    'VALID_LEGAL_ADDRESS_FORMAT': '地址字段不合法',
    'FIELD_ANOMALY': '数据波动性',
    'FIELD_CONTINUITY': '数据连续性'
}

DASH_BOARD_RESULT_CODE = {
    'EXCEPTION': 1,  # 检查异常
    'REGULAR': 0  # 检查正常
}

BIGQUERY_CLIENT = {
    'XED_CLIENT': 'xed_client',  # xed-project
    'FOOTPRINT_CLIENT': 'footprint_client'  # footprint
}

ALICLOUD_OSS_CONFIG = {
    'ACCESSKEY_ID': 'ACCESSKEY_ID',
    'ACCESSKEY_SECRET': 'ACCESSKEY_SECRET'
}

BUSINESS_TYPE = {
    'LENDING': 'lending',
    'MINTING': 'minting',
    'DEX': 'dex',
    'FARMING': 'farming'
}

BUSINESS_SECOND_TYPE = {
    'BORROW': 'borrow',
    'REPAY': 'repay',
    'SUPPLY': 'supply',
    'WITHDRAW': 'withdraw',
    'LIQUIDATION': 'liquidation',
    'SWAP': 'swap',
    'ADD_LIQUIDITY': 'add_liquidity',
    'REMOVE_LIQUIDITY': 'remove_liquidity',
    'REWARD': 'reward',
    'POSITION': 'position',
}

BUSINESS_TYPE_WITH_SECOND_TYPE = {
    BUSINESS_TYPE['LENDING']: [
        BUSINESS_SECOND_TYPE['BORROW'], BUSINESS_SECOND_TYPE['REPAY'],
        BUSINESS_SECOND_TYPE['SUPPLY'], BUSINESS_SECOND_TYPE['WITHDRAW'],
        BUSINESS_SECOND_TYPE['LIQUIDATION'],
    ],
    BUSINESS_TYPE['MINTING']: [
        BUSINESS_SECOND_TYPE['BORROW'], BUSINESS_SECOND_TYPE['REPAY'],
        BUSINESS_SECOND_TYPE['SUPPLY'], BUSINESS_SECOND_TYPE['WITHDRAW'],
        BUSINESS_SECOND_TYPE['LIQUIDATION'],
    ],
    BUSINESS_TYPE['DEX']: [
        BUSINESS_SECOND_TYPE['SWAP'],
        BUSINESS_SECOND_TYPE['ADD_LIQUIDITY'],
        BUSINESS_SECOND_TYPE['REMOVE_LIQUIDITY'],
    ]
}

CHAIN = {
    'POLYGON': 'polygon',
    'BSC': 'bsc',
    'ETHEREUM': 'ethereum',
    'XDAI': 'xdai',
    'AVALANCHE' : 'avalanche',
    'ARBITRUM': 'arbitrum',
    'FANTOM': 'fantom'
}

PROJECT_PATH=os.path.split(os.path.abspath(os.path.dirname(__file__)))[0]


DEFI360_CHAIN = {
    'ETHEREUM': 'Ethereum',
    'BSC': 'BSC',
    'POLYGON': 'Polygon',
    'AVALANCHE': 'Avalanche',
}

BIGQUERY_EXPORT_DATA_TASK_STATUS = {
    'FINISHED': 'FINISHED',
    'WAIT_LOADING': 'WAIT_LOADING',
    'LOADING': 'LOADING',
    'PENDING': 'PENDING',
    'CANCELLED': 'CANCELLED',
    'ETL': 'ETL'
}

ENVIRONMENT = os.environ.get('ENVIRONMENT')
