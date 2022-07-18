from inner_validate.validate_cls.base_validate import BaseValidate
from utils.query_bigquery import query_bigquery
import pandas as pd

class DataNullValidate(BaseValidate):
    valid_rule_name = 'FIELD_LEGAL_NULL'

    def validate_result(self, valid_sql, info):
        df = query_bigquery(valid_sql)
        result = [
            {'rule_name': self.valid_rule_name, 'field': item.get('field'),
             'num': len(df[pd.isna(df[item.get('field')])])}
            for item in info if not
            df[pd.isna(df[item.get('field')])].empty]
        return result
