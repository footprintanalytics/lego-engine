# 使用说明 https://vmwt14.yuque.com/vmwt14/uybn6l/om69hw
from utils.bigquery_utils import create_view, get_bigquery_client
from utils.constant import PROJECT_PATH

test_format = ['autoparse_alpha', 'autoparse_beta', 'gaia_dao_test']


def _get_sql_path(view_name: str, is_test: bool or None = None):
    if is_test is not None:
        return 'test_env_sql' if is_test else 'sql'
    for _test_format in test_format:
        if _test_format in view_name:
            return 'test_env_sql'
    return 'sql'


def create_or_update_view(view_name: str, project='common', is_test: bool or None = None):
    with open(f'{PROJECT_PATH}/defi360/bigquery_view/{_get_sql_path(view_name, is_test)}/{project}/{view_name}.sql') as f:
        sql = f.read()
        create_view(get_bigquery_client(), sql, view_name)


if __name__ == '__main__':
    # create_or_update_view('gaia-dao.gaia_dao_test.token_price_daily_100d', project='ForTube', is_test=True)
    create_or_update_view('gaia-dao.gaia_dao_test.token_price_daily_100d')