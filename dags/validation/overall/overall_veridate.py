import os, sys

project_path = os.path.split(os.path.split(os.path.split(os.path.abspath(os.path.dirname(__file__)))[0])[0])[0]
sys.path.append(f'{project_path}/dags')
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = f'{project_path}/../bigquery-key.json'

import getopt
from utils.constant import BUSINESS_TYPE
from validation.overall.dex import project_map as dex_project_map
from validation.overall.lending import project_map as lending_project_map


def run(project_map, cfg):
    validate_cls = project_map['default']
    project = cfg['project']
    if project in project_map:
        validate_cls = project_map[project]
    print(f'overall_veridate.py run() ==== run project:{project} cls:{validate_cls} cfg:{cfg}')
    validate_cls(cfg).process()


def run_by_chain_project_business_type(chain, project, business_type):
    cfg = {'chain': chain, 'project': project, 'business_type': business_type}
    business_type_map = {
        'dex': dex_project_map,
        'lending': lending_project_map
    }
    if business_type not in business_type_map:
        raise Exception('not find business_type_map:{}'.format(business_type))
    project_map = business_type_map[business_type]
    run(project_map, cfg)


def format_chain(chain):
    if chain is None:
        return chain
    chain_mapping = {
        'ethereum': 'ethereum',
        'bsc': 'bsc',
        'polygon': 'polygon'
    }

    if chain in chain_mapping:
        return chain_mapping[chain]
    return chain


if __name__ == '__main__':
    chain = None
    project = None
    business_type = None

    try:
        opts, args = getopt.getopt(sys.argv[1:], "c:p:b:", ["chain=", "project=", 'business_type='])
    except getopt.GetoptError:
        print('-c <chain> -p <project> -b <business_type>')
        sys.exit(2)
    # 读取参数
    for opt, arg in opts:
        if opt in ('-c', '--chain'):
            chain = format_chain(arg.lower())
        elif opt in ('-p', '--project'):
            project = arg.lower()
        elif opt in ('-b', '--business_type'):
            business_type = arg.lower()
    # 参数检查
    if chain is None:
        print('chain is None use -c or --chain')
        sys.exit(2)
    if project is None:
        print('project is None use -p or --project')
        sys.exit(2)
    if business_type is None or business_type not in BUSINESS_TYPE.values():
        print('business_type is None use -b or --business_type')
        sys.exit(2)
    # 执行脚本
    run_by_chain_project_business_type(chain, project, business_type)
