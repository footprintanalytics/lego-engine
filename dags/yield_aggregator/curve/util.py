import json

import pandas as pd


def lower(obj):
    obj['pool_address'] = obj['pool_address'].lower()
    obj['token_address'] = obj['token_address'].lower()
    return obj

def json_to_csv():
    with open('./curve_v1_pools.json', 'r') as f:
        data = json.load(f)
        df = pd.DataFrame(data, columns=['pool_address', 'pool_name', 'pool_symbol', 'token_symbol', 'token_address',
                                         'decimals'])
        print(data)
        df = df.apply(lower, axis=1)
        df.to_csv('./curve_v1_pools.csv', index=False)


def gen_pools_info():
    df = pd.read_csv('./curve_v1_pools.csv')
    df = df[['pool_address', 'pool_symbol', 'pool_name']]
    df = df.rename(columns={
        'pool_address': 'address',
        'pool_symbol': 'name',
        'pool_name': 'description'
    })
    df = df.drop_duplicates()
    print(df)
    df.to_csv('./curve_v1_pools_info.csv', index=False)
    pass


if __name__ == '__main__':
    json_to_csv()
    # gen_pools_info()
