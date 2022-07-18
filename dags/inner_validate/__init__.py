from inner_validate.lending_validate import LendingValidate
from inner_validate.dex_validate import DexValidate
from utils.print_util import print_blue, print_green, print_red
from utils.constant import PROJECT_PATH
import sys, os, re, getopt

chain_mapping = {
    'ethereum': 'ethereum',
    'bsc': 'binance',
    'polygon': 'polygon'
}

def get_platform_info(folder_path):
    platform_info = []
    for folder in os.listdir(folder_path):
        platform_path = os.path.join(folder_path, folder)
        if "doc_image" != folder and not folder.startswith('_') and os.path.isdir(platform_path) and chain_mapping.get(folder):
            platform_info += [{'name': c_folder, 'chain': chain_mapping[folder]} for c_folder in os.listdir(platform_path) if
                              os.path.isdir(os.path.join(platform_path, c_folder)) and not c_folder.startswith('_')]
    print(platform_info)
    return platform_info


def get_lending_model_info():
    return get_platform_info(folder_path=PROJECT_PATH + '/lending')


def get_dex_model_info():
    return get_platform_info(folder_path=PROJECT_PATH + '/dex')




# 可以传入参数,可以跑验证平台
# 定义一个命令行的脚本
if __name__ == '__main__':
    lending_info = get_lending_model_info()
    dex_info = get_dex_model_info()
    args = sys.argv[1:]
    opts, args = getopt.getopt(args, 'p:c:')
    platform_name = None
    chain_name = None
    for opt, arg in opts:
        if opt in ('-p'):
            platform_name = arg
        if opt in ('-c'):
            chain_name = arg
    result_list = []
    print(platform_name,chain_name)
    for info in lending_info:
        name, chain = info.get('name'), info.get('chain')
        if not platform_name or (platform_name == name and chain == chain_name):
            print_blue('lending {} validate start'.format(chain + '-' + name))
            result_list += LendingValidate(name, chain=chain).validate()
            print_blue('lending {} validate finish'.format(chain + '-' + name))

    for info in dex_info:
        name, chain = info.get('name'), info.get('chain')
        if not platform_name or (platform_name == name and chain == chain_name):
            print_blue('dex {} validate start'.format(chain + '-' + name))
            result_list += DexValidate(name, chain=chain).validate()
            print_blue('dex {} validate finish'.format(chain + '-' + name))

    result_fail_list = list((filter(lambda n: n.get('result_code'), result_list)))

    print('{color} total: {total},success: {success},fail: {fail}'
        .format(
        color="\033[91m" if len(result_fail_list) > 0 else "\033[92m",
        total=len(result_list),
        success=len(result_list) - len(result_fail_list),
        fail=len(result_fail_list)
    ))
