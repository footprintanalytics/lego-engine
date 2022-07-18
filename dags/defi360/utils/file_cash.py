
import os
import pandas as pd
from utils.constant import PROJECT_PATH


def ensure_dir(file_path):
    directory = os.path.dirname(file_path)
    if not os.path.exists(directory):
        try:
            os.makedirs(directory)
        except FileExistsError:
            print('已存在目录,跳过')


class FileCache:
    def exist_cash_name(self, path):
        path = self.get_csv_name(path)
        print("path: ", path)
        return os.path.exists(path)

    def get_csv_name(self, path):
        dags_folder = PROJECT_PATH
        return os.path.join(dags_folder, '{path}'.format(path=path))

    # 查询内容缓存到文件
    def cash_csv_data(self, df, path):
        ensure_dir(self.get_csv_name(path))
        print('save csv file to ', self.get_csv_name(path))
        df.to_csv(self.get_csv_name(path), index=False)

    # 删除csv
    def remove_cash_csv(self, path):
        path = self.get_csv_name(path)
        os.remove(path)

    # 获取csv数据
    def get_csv_data(self, path):
        path = self.get_csv_name(path)
        return pd.read_csv(path)
