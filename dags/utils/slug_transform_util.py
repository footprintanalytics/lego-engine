"""请求出去的时候要转成第三方平台的名称"""


def transform_slug_to_out(slug):
    if slug == 'uniswap-v2':
        return 'uniswap'
    return slug


"""保存数据时要转成内部系统数据"""


def tranform_slug_from_out(slug):
    if slug == 'uniswap':
        return 'uniswap-v2'
    return slug
