import requests

from models import ValidateRecordModel
import pydash
from utils.gql.gql_monitor_validate_record import GQLMonitorValidateRecord


members = {
    'Herwin': 'U010HS22HNV',
    'Leo': 'U010VF8JF9C',
    'Jakin': 'U010W22C1JQ',
    'Aison': 'U010W410HR6'
}


def send_to_slack(text):
    url = 'https://'
    requests.post(url=url, json={'text': text})


def send_to_slack_and_remind_all(text):
    send_to_slack(text + '\n <!channel>')


def save_monitor(data: dict, slack_warn: bool):
    if slack_warn and data["result_code"] == 1:
        other_msg = f',缺失日期：{pydash.get(data, "middle_output.miss_date_info")}' if 'data_continuity' in data[
            'validate_name'] else ''
        send_to_slack(f'{data["project"]} 平台 {data["validate_name"]} 规则校验失败：{data["desc"]} {other_msg}')
        data["has_warn"] = True
    res = ValidateRecordModel.insert_one(data)

    gql_result = GQLMonitorValidateRecord().insertMonitorValidateRecord(data)

    print(gql_result)

    data['_id'] = pydash.get(res, 'inserted_id')
    return data

