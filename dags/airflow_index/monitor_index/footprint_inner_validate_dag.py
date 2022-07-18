# from inner_validate import get_dex_model_info, get_lending_model_info
# from inner_validate.lending_validate import LendingValidate
# from inner_validate.dex_validate import DexValidate
# from utils.build_dag_util import BuildDAG
#
#
#
# for lending_info in get_lending_model_info():
#     name, chain = lending_info.get('name'), lending_info.get('chain')
#     validate = LendingValidate(name, chain)
#     dag_id = 'footprint_validate_{}_dag'.format(validate.task_name)
#     globals()[dag_id] = BuildDAG().build_dag_with_ops(dag_params=validate.airflow_dag_params(), ops=[validate.validate])
#
# for dex_info in get_dex_model_info():
#     name, chain = dex_info.get('name'), dex_info.get('chain')
#     validate = DexValidate(name, chain)
#     dag_id = 'footprint_validate_{}_dag'.format(validate.task_name)
#     globals()[dag_id] = BuildDAG().build_dag_with_ops(dag_params=validate.airflow_dag_params(), ops=[validate.validate])
