import os
import glob
import pandas as pd
from joblib import Parallel, delayed


def ensure_dir(file_path):
    directory = os.path.dirname(file_path)
    if not os.path.exists(directory):
        try:
            os.makedirs(directory)
        except FileExistsError:
            print('已存在目录,跳过')

class CSVUtil:
    @staticmethod
    def merge_csv(csv_path_ls: [str], output_path: str):
        # print('merge {} csv to {}'.format(len(csv_path_ls), output_path))
        # 移除掉合并的文件
        if os.path.exists(output_path):
            os.remove(output_path)
        # 没文件则不合并
        if len(csv_path_ls) == 0:
            return
        not_empty_df = []
        result = Parallel(n_jobs=5, backend='multiprocessing')(
            delayed(CSVUtil.read_csv)(f) for f in csv_path_ls)
        for d in result:
            if d is not None:
                not_empty_df.append(d)
        combined_csv = pd.concat(not_empty_df)
        combined_csv.to_csv(output_path, index=False)

    @staticmethod
    def read_csv(file: str):
        df = pd.read_csv(file)
        if not df.empty:
            return df
        return None

