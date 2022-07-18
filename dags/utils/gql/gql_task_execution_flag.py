from utils.gql.gql_basic import GraphqlBasic
from utils.date_util import DateUtil


class GQLTaskExecutionFlag(GraphqlBasic):

    def insertTaskExecutionFlag(self, variables):
        operationDoc = self.insertTaskTaskExecutionOperationDoc()
        operationName = 'insert_single_task_execution_flag'
        if variables['data_last_time']:
            variables['data_last_time_value'] = variables['data_last_time'].strftime('%Y%m%d')
            variables['data_last_time'] = variables['data_last_time'].strftime('%Y-%m-%d %H:%M:%S')

        if variables['last_updated_at']:
            variables['last_updated_at'] = variables['last_updated_at'].strftime('%Y-%m-%d %H:%M:%S')

        if not 'data_from' in variables.keys():
            variables['data_from'] = 'footprint'

        print(variables)
        return self.fetchGraphql(operationDoc, operationName, variables)

    def insertTaskTaskExecutionOperationDoc(self):
        operationDoc = """mutation insert_single_task_execution_flag(
                    $task_name: String
                    $association_table: String
                    $category: String
                    $data_last_time: timestamptz
                    $data_last_time_value: Int
                    $last_updated_by: String
                    $last_updated_at: timestamptz
                    $data_from: String
                ) {
                    insert_indicator_task_execution_flag_one(object: 
                    {
                        task_name: $task_name,
                        association_table: $association_table,
                        category: $category,
                        data_last_time: $data_last_time,
                        data_last_time_value: $data_last_time_value,
                        last_updated_by: $last_updated_by,
                        last_updated_at: $last_updated_at,
                        data_from: $data_from
                    }, on_conflict: {
                        constraint: task_execution_flag_task_name_association_table_key,
                        update_columns: [data_last_time, category, data_last_time_value, last_updated_by, last_updated_at, data_from]
                    }) {
                        task_name,
                        association_table,
                        category,
                        data_last_time,
                        data_last_time_value,
                        last_updated_by,
                        last_updated_at,
                        data_from
                    }
                }"""
        return operationDoc


if __name__ == '__main__':
    variables = {
        "task_name": "token_daily_stats",
        "association_table": "token_daily_stats",
        "category": "token",
        "data_last_time": DateUtil.utc_start_of_date(),
        "last_updated_by": "Jakin",
        "last_updated_at": DateUtil.utc_start_of_date()
    }
    GQLTaskExecutionFlag().insertTaskExecutionFlag(variables)
