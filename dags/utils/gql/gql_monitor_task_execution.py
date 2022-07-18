from utils.gql.gql_basic import GraphqlBasic
from utils.date_util import DateUtil


class GQLMonitorTaskExecution(GraphqlBasic):

    def insertMonitorTaskExecution(self, variables):

        print(variables)
        operationDoc = self.insertMonitorTaskExecutionOperationDoc()
        operationName = 'insert_single_monitor_task_execution'
        variables['stats_date'] = variables['stats_date'].strftime('%Y-%m-%d')
        if variables['desc']:
            variables['desc_en'] = variables['desc']
            variables.pop('desc')
        if 'updatedAt' in variables.keys() and variables['updatedAt']:
            variables.pop('updatedAt')

        if not 'data_from' in variables.keys():
            variables['data_from'] = 'footprint'
        return self.fetchGraphql(operationDoc, operationName, variables)

    def insertMonitorTaskExecutionOperationDoc(self):
        operationDoc = """mutation insert_single_monitor_task_execution(
            $field: String
            $rule_name: String
            $stats_date: timestamptz
            $task_name: String
            $database_name: String
            $desc_en: String
            $desc_cn: String
            $item_value: Int
            $result_code: Int
            $sql: String
            $table_name: String
            $data_from: String
        ) {
            insert_indicator_monitor_task_execution_one(object: 
            {
                field: $field,
                rule_name: $rule_name,
                stats_date: $stats_date,
                task_name: $task_name,
                database_name: $database_name,
                desc_en: $desc_en,
                desc_cn: $desc_cn,
                item_value: $item_value,
                result_code: $result_code,
                sql: $sql,
                table_name: $table_name,
                data_from: $data_from
            }, on_conflict: {
                constraint: monitor_task_execution_task_name_rule_name_field_stats_date_key,
                update_columns: [database_name, table_name, desc_en, desc_cn, item_value, result_code, sql, data_from]
            }) {
                field,
                rule_name,
                stats_date,
                task_name,
                database_name,
                desc_en,
                desc_cn,
                item_value,
                result_code,
                sql,
                table_name
            }
        }"""
        return operationDoc


if __name__ == '__main__':
    variables = {
        "field": "",
        "rule_name": "task_execution",
        "stats_date": DateUtil.utc_start_of_date(),
        "task_name": "sushi_dex_remove_liquidity_transaction_flow",
        "database_name": "footprint_etl",
        "desc_en": "sushi_dex_remove_liquidity rule_name is task execution",
        "desc_cn": "任务有效性",
        "item_value": 0,
        "result_code": 0,
        "sql": "",
        "table_name": "aaa",
    }
    print(variables)
    result = GQLMonitorTaskExecution().insertMonitorTaskExecution(variables)
    print(result.text)
