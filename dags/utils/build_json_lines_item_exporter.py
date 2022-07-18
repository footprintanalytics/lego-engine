import decimal
from json import JSONEncoder


def EncodeDecimal(o):
    if isinstance(o, decimal.Decimal):
        return float(round(o, 8))
    raise TypeError(repr(o) + " is not JSON serializable")


def build_json_lines_item_export(file, items):
    encoder = JSONEncoder(default=EncodeDecimal)
    for item in items:
        data = encoder.encode(item) + '\n'
        file.write(data)
