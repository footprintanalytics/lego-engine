from utils.template.base import BaseTemplate
from utils.constant import BUSINESS_TYPE, CHAIN, BUSINESS_SECOND_TYPE, PROJECT_PATH

business_type = BUSINESS_TYPE['MINTING']
if __name__ == '__main__':
    project = 'abracadabra'
    chain = CHAIN['AVALANCHE']
    t = BaseTemplate(project=project, chain=chain, business_type=business_type, sql_temp_paths={
        BUSINESS_SECOND_TYPE['LIQUIDATION']: f'{PROJECT_PATH}/utils/template/minting/minting_sql_template.txt'
    })
    t.gen_template()
