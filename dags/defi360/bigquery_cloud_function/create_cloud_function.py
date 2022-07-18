# 使用说明: https://vmwt14.yuque.com/vmwt14/uybn6l/hxkwsu
from utils.bigquery_utils import query, get_bigquery_client
from utils.constant import PROJECT_PATH

test_format = ['autoparse_alpha', 'autoparse_beta', 'gaia_dao_test']


def _get_sql_path(view_name: str, is_test: bool or None = None):
    if is_test is not None:
        return 'test_env_sql' if is_test else 'sql'
    for _test_format in test_format:
        if _test_format in view_name:
            return 'test_env_sql'
    return 'sql'


def create_function(function_name: str, project: str = 'common', is_test: bool or None = None):
    with open(f'{PROJECT_PATH}/defi360/bigquery_cloud_function/{_get_sql_path(function_name, is_test)}/{project}/{function_name}.sql') as f:
        sql = f.read()
        query(get_bigquery_client(), sql)


if __name__ == '__main__':
    create_function('footprint-etl.Ethereum_ForTube_autoparse.WithdrawUnderlying_event')
