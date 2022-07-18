from defi_protocol.upload_logo_to_oss_update_mongo import upload_log_to_oss_update_mongo
import requests, pydash
from datetime import datetime
from models import DefiProtocolInfo
from defi_protocol.defi_llama.defi_llama_defi_info import update_defi_info_data

defi_llama = 'https://api.llama.fi/protocols'
end_point = 'oss-us-east-1.aliyuncs.com'
bucket = 'bucket_name'
# bucket = 'acer-test'
images_type = 'png'

def fix_logo():
    upload = upload_log_to_oss_update_mongo(
        end_point=end_point,
        bucket_name=bucket,
        images_type=images_type
    )
    result = DefiProtocolInfo.find({})
    defi_llama_result = requests.get(defi_llama)
    data: list = defi_llama_result.json()
    checked_slug = []
    for i in result:
        source_logo = pydash.get(i, 'source_logo')
        slug = pydash.get(i, 'slug')
        coin_gecko_id = pydash.get(i, 'coin_gecko_id')
        if slug in checked_slug:
            print(f'skip {slug}')
            continue
        else:
            checked_slug.append(slug)
            if source_logo:
                res = requests.get(source_logo)
                if res.status_code != 200:
                    print(f'{slug} get error url {source_logo}')
                    if coin_gecko_id:
                        print(f'coin_gecko_id: {coin_gecko_id}')
                        data_item = pydash.find(data, {'gecko_id': coin_gecko_id})
                    else:
                        data_item = pydash.find(data, {'slug': slug})
                    print(data_item)
                    new_logo = pydash.get(data_item, 'logo')
                    print(f'new logo {new_logo}')
                    upload.singe_update_exec(new_logo, slug)
                else:
                    continue
            else:
                print(f'can not get {slug} source_logo')
                if coin_gecko_id:
                    print(f'coin_gecko_id: {coin_gecko_id}')
                    data_item = pydash.find(data, {'gecko_id': coin_gecko_id})
                else:
                    data_item = pydash.find(data, {'slug': slug})
                print(data_item)
                new_logo = pydash.get(data_item, 'logo')
                print(f'new logo {new_logo}')
                upload.singe_update_exec(new_logo, slug)

def whole_reload():
    upload = upload_log_to_oss_update_mongo(
        end_point=end_point,
        bucket_name=bucket,
        images_type=images_type
    )
    result = DefiProtocolInfo.find({})
    defi_llama_result = requests.get(defi_llama)
    data: list = defi_llama_result.json()
    load_slug_list = []
    for i in result:
        slug = pydash.get(i, 'slug')
        if slug in load_slug_list:
            print(f'skip {slug}')
            continue
        coin_gecko_id = pydash.get(i, 'coin_gecko_id')
        if coin_gecko_id:
            print(f'coin_gecko_id: {coin_gecko_id}')
            data_item = pydash.find(data, {'gecko_id': coin_gecko_id})
        else:
            data_item = pydash.find(data, {'slug': slug})
        if data_item is None:
            print(f'{slug} logo is null')
            query = {
                "slug": slug
            }
            update = {
                "logo": "",
                "updatedAt": datetime.now(),
                "source_logo": ""
            }
            DefiProtocolInfo.update_many(query=query, set_dict=update)
        else:
            new_logo = pydash.get(data_item, 'logo')
            upload.singe_update_exec(new_logo, slug)
            load_slug_list.append(slug)
    # update_defi_info_data()

def fix_singe_logo():
    upload = upload_log_to_oss_update_mongo(
        end_point=end_point,
        bucket_name=bucket,
        images_type=images_type
    )
    upload.singe_update_exec('https://icons.llama.fi/uniswap.png', 'uniswap-v3')

if __name__ == '__main__':
    # fix_singe_logo()
    fix_logo()
    update_defi_info_data()
