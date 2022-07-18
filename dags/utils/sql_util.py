class SQLUtil:
    @staticmethod
    def build_sql_array(arr: list):
        return '({})'.format(',\n'.join(map(lambda s: "'{}'".format(s), arr)))

    @staticmethod
    def build_match_tokens(tokens):
        tokens = [token.lower() if isinstance(token, str) else token for token in tokens]
        if len(tokens) == 1:
            return " = '{token}' ".format(token=''.join(tokens))
        else:
            return " in {tokens} ".format(tokens=SQLUtil.build_sql_array(tokens))
