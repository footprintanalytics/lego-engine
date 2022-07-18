from utils import Constant
import oss2

class AliCloudOSS:
    def __init__(self, end_point:str, bucket: str, session=None):
        config = Constant.ALICLOUD_OSS_CONFIG
        self.end_posint = end_point
        self.bucket_name = bucket
        self.auth = oss2.Auth(config['ACCESSKEY_ID'], config['ACCESSKEY_SECRET'])
        self.bucket = oss2.Bucket(self.auth, end_point, bucket_name=bucket, session=session)
        self.acl_map = {
            'DEFAULT': oss2.OBJECT_ACL_DEFAULT,
            'PRIVATE': oss2.OBJECT_ACL_PRIVATE,
            'PUBLIC_READ': oss2.OBJECT_ACL_PUBLIC_READ,
            'PUBLIC_READ_WRITE': oss2.OBJECT_ACL_PUBLIC_READ_WRITE
        }
        print(f'get {bucket}!')

    # 上传本地文件
    def upload_local_file_to_oss(self, oss_file_name: str, oss_folder_name: str, local_file_path: str, permission='PUBLIC_READ'):
        file_url = 'https://{bucket_name}.{end_point}/{oss_folder_name}/{oss_file_name}'.format(end_point=self.end_posint, bucket_name=self.bucket_name, oss_folder_name=oss_folder_name,oss_file_name=oss_file_name)
        # https://acer-test.oss-us-east-1.aliyuncs.com/convergence.png
        str_oss_name = f'{oss_folder_name}/{oss_file_name}'
        try:
            self.bucket.put_object_from_file(key=str_oss_name, filename=local_file_path)
            print(f'upload oss success: {file_url}')
            self.put_object_acl(str_oss_name, permission)
            print(f'upload oss permission acl success: {file_url}')
            return file_url
        except Exception as e:
            print(f'upload file error: {e}')
            return

    # 上传文件夹
    def upload_folder_to_oss(self, oss_folder_name: str):
        folder_url = '{end_point}/{bucket_name}/{oss_folder_name}'.format(end_point=self.end_posint, bucket_name=self.bucket_name,
                                                                 oss_folder_name=oss_folder_name)
        self.bucket.enable_crc = False
        try:
            self.bucket.put_object(oss_folder_name, None)
            print(folder_url)
            return folder_url
        except Exception as e:
            print(e)
            return

    # 获取文件信息
    def get_object_info(self, oss_file_name: str):
        if self.bucket.object_exists():
            result = self.bucket.head_object(key=oss_file_name)
            return result
        else:
            print('file not found')
            return

    # 文件权限设置
    def put_object_acl(self, oss_file_name, permission: str):
        self.bucket.put_object_acl(oss_file_name, self.acl_map[permission])
        return

if __name__ == '__main__':
    a = AliCloudOSS(end_point='oss-us-east-1.aliyuncs.com', bucket='acer-test')
    a.upload_local_file_to_oss('logo_images/1.png', 'logo_images','../logo_images/kava-lend.png')