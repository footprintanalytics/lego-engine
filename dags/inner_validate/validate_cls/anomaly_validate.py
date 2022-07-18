from inner_validate.validate_cls.base_validate import BaseValidate
from utils.query_bigquery import query_bigquery


class AnomalyValidate(BaseValidate):
    valid_rule_name = 'FIELD_ANOMALY'

    def validate_result(self,valid_sql,fields_info):
        df = query_bigquery(valid_sql)
        result = [
            {'rule_name': self.valid_rule_name, 'field': info.get('field'), 'num': len(df[df[info.get('field')] == 1])}
            for info in fields_info if not
            df[df[info.get('field')] == 1].empty]
        return result