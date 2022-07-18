from utils.gql.gql_basic import GraphqlBasic
from utils.date_util import DateUtil


class GQLMonitorValidateRecord(GraphqlBasic):

    def insertMonitorValidateRecord(self, variables):
        operationDoc = self.insertMonitorValidateRecordOperationDoc()
        operationName = 'insert_single_monitor_validate_record'
        if variables['stats_start']:
            variables['stats_start'] = variables['stats_start'].strftime('%Y-%m-%d %H:%M:%S')
        if variables['stats_end']:
            variables['stats_end'] = variables['stats_end'].strftime('%Y-%m-%d %H:%M:%S')
        if variables['desc']:
            variables['validate_desc'] = variables['desc']
            variables.pop('desc')

        if not 'data_from' in variables.keys():
            variables['data_from'] = 'footprint'
        return self.fetchGraphql(operationDoc, operationName, variables)

    def insertMonitorValidateRecordOperationDoc(self):
        operationDoc = """mutation insert_single_monitor_validate_record(
                    $project: String
                    $chain: String
                    $validate_type: String
                    $validate_name: String
                    $validate_date: String
                    $validate_args: json
                    $validate_table: String
                    $validate_desc: String
                    $stats_start: timestamptz
                    $stats_end: timestamptz
                    $execute_time: numeric
                    $middle_output: json
                    $result_code: Int
                    $result_message: String
                    $has_warn: Boolean,
                    $data_from: String
                ) {
                    insert_indicator_monitor_validate_record_one(object: 
                    {
                        project: $project,
                        chain: $chain,
                        validate_type: $validate_type,
                        validate_name: $validate_name,
                        validate_date: $validate_date,
                        validate_args: $validate_args,
                        validate_table: $validate_table,
                        validate_desc: $validate_desc,
                        stats_start: $stats_start,
                        stats_end: $stats_end,
                        execute_time: $execute_time,
                        middle_output: $middle_output,
                        result_code: $result_code,
                        result_message: $result_message,
                        has_warn: $has_warn,
                        data_from: $data_from
                    }) {
                        project,
                        chain,
                        validate_type,
                        validate_name,
                        validate_date,
                        validate_args,
                        validate_desc,
                        stats_start,
                        stats_end,
                        execute_time,
                        middle_output,
                        data_from
                    }
                }"""
        return operationDoc


if __name__ == '__main__':
    variables = {
        "project": "sushi",
        "chain": "ethereum",
        "validate_type": "basic",
        "validate_name": "data_continuity_dex_liquidity_volume_validate",
        "validate_date": "2022-02-06",
        "validate_args": {
            "validate_table": "footprint-etl.ethereum_dex_sushi.sushi_dex_remove_liquidity_all",
            "chain": "ethereum",
            "project": "sushi"
        },
        "validate_table": "footprint-etl.ethereum_dex_sushi.sushi_dex_remove_liquidity_all",
        "validate_desc": "dex liquidity业务的volume的连续性校验",
        "stats_start": DateUtil.utc_x_hours_ago(1),
        "stats_end": DateUtil.utc_current(),
        "execute_time": 9.529696941375732,
        "middle_output": {
            "validate_field": "volume",
            "validate_table": "footprint-etl.ethereum_dex_sushi.sushi_dex_remove_liquidity_all",
            "validate_count": 0,
            "miss_date_info": [],
            "sql": "\n        with dex_liquidity_daily as (\n            select\n            day,\n            protocol_id,  \n            sum(usd_amount) as volume\n            from(\n                select\n                Date(k.block_time) as day,\n                k.token_address,\n                k.protocol_id,\n                SAFE_MULTIPLY(k.token_price, k.token_amount) as usd_amount\n                from (\n                    select\n                        t.block_time,\n                        t.token_address,\n                        t.protocol_id,\n                        token_price,\n                        token_amount,\n                        from(\n                            select t0.*,\n                            a.price,\n                            a.price AS token_price,\n                            from (select * from footprint-etl.ethereum_dex_sushi.sushi_dex_remove_liquidity_all where Date(block_time) between '2021-11-08' and '2022-02-06')  t0 \n                            left join (select * from `footprint-etl-internal.view_to_table.fixed_price` where Date(timestamp) between '2021-11-08' and '2022-02-06')a \n                            on lower(t0.token_address) = lower(a.address)          \n                            and (TIMESTAMP_SECONDS(div(UNIX_SECONDS(safe_cast(t0.block_time as  TIMESTAMP)), 300) * 300)) = a.timestamp \n                        )  t\n                    )k\n                ) group by 1,2\n                having volume is not null\n        )\n        select\n        *,\n        concat(date_add(day,interval 1 day),' ~ ', date_add(next_day,interval -1 day)) as loss_date_data\n        from ( \n            select\n            day,\n            lead(day, 1, Date(current_timestamp())) OVER (PARTITION BY protocol_id ORDER BY day) AS next_day,\n            protocol_id\n            from dex_liquidity_daily \n        )\n        where date_diff(next_day,day,day) >1\n\n        "
        },
        "result_code": 0,
        "result_message": "校验成功",
        "has_warn": False
    }
    result = GQLMonitorValidateRecord().insertMonitorValidateRecord(variables)
    print(result.text)
