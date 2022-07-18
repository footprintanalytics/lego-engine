from utils.template.base import BaseTemplate
from utils.constant import BUSINESS_TYPE, PROJECT_PATH, BUSINESS_SECOND_TYPE, CHAIN

business_type = BUSINESS_TYPE['LENDING']

if __name__ == '__main__':
    project = 'alpha1'
    chain = CHAIN['ETHEREUM']
    t = BaseTemplate(project=project, chain=chain, business_type=business_type, sql_temp_paths={
        BUSINESS_SECOND_TYPE['LIQUIDATION']: f'{PROJECT_PATH}/utils/template/lending/liquidation_sql_template.txt'
    })
    t.gen_template()

