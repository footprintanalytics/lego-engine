from utils.date_util import DateUtil
from utils import Constant
from models import RequestLog
import pydash


def request_and_save_response(func, params):

    request_time = DateUtil.utc_current()
    service = pydash.get(params, 'service')
    method = pydash.get(params, 'method', 'GET')
    url = pydash.get(params, 'url', '')
    query = pydash.get(params, 'query', {})
    headers = pydash.get(params, 'headers', {})
    body = pydash.get(params, 'body', {})
    is_form = pydash.get(params, 'is_form', False)
    request_log = {
        "service": service,
        "request_time": request_time,
        "method": method,
        "url": url,
        "query": query,
        "headers": headers,
        "body": body,
        "is_form": is_form,
        "status": Constant.PROXY_LOG_STATUS["PENDING"]
    }
    result = {}

    try:
        result = func()
        request_log["response"] = result
        request_log["status"] = Constant.PROXY_LOG_STATUS["FINISH"]
    except Exception as e:
        print('request log fail', e.args[0])
        request_log["status"] = Constant.PROXY_LOG_STATUS["ERROR"]
        request_log['response'] = {
            "message": e.args[0],
            "body": e.args
        }

    request_log["response_time"] = DateUtil.utc_current()
    request_log['useTime'] = 100
    RequestLog.insert_one(request_log)
    return result
