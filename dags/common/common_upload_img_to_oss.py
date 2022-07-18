import requests
from config import project_config
from utils.alicloud_oss import AliCloudOSS

end_point = 'oss-us-east-1.aliyuncs.com'
bucket_name = 'bucket_name'
default_image_type = 'png'
effective_image_type = ['png', 'svg', 'jpg', 'bmp', 'raw', 'webp', 'tif', 'gif', 'apng']
dags_path = f'{project_config.dags_folder}/logo_images'
headers = {
    'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/95.0.4638.54 Safari/537.36'
}
ali_oss = AliCloudOSS(end_point, bucket_name)


def common_upload_img_to_oss(slug: str, source_logo: str):
    try:
        oss_path = download_logo_by_url(slug, source_logo)
    except Exception as e:
        oss_path = None
        print('common_upload_img_to_oss 失败, err:'+str(e))
    return oss_path


def download_logo_by_url(slug: str, source_logo: str):
    slug = slug.lower().strip('/').replace(' / ', '_').replace('/', '_').replace(' ', '_')
    if not source_logo:
        return
    response = None
    try:
        response = requests.get(url=source_logo, headers=headers)
    except Exception as e:
        print(f'failed to download logo, error: {e}')
    if not response:
        return None
    image_type = str(response.content[:3])
    if image_type == "b'GIF'":
        image_type = 'gif'
    elif source_logo[-3:] in effective_image_type:
        image_type = source_logo[-3:]
    else:
        image_type = default_image_type
    file_path = f'{dags_path}/{slug}.{image_type}'
    with open(file_path, 'wb') as file:
        file.write(response.content)
        file.close()
    oss_path = upload_img_to_oss(slug, image_type, file_path)
    return oss_path


def upload_img_to_oss(slug: str, image_type: str, file_path: str):
    oss_file_name = f'{slug}.{image_type}'
    oss_path = ali_oss.upload_local_file_to_oss(oss_file_name, 'logo_images', file_path)
    return oss_path


def main():
    test = common_upload_img_to_oss('AAAA', 'https://cryptoslam-token-images.s3.amazonaws.com/icons/hashes-3.png')
    print(test)


if __name__ == '__main__':
    main()
