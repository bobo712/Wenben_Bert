import os
import pandas as pd


def merge_csv_files(folder_path):
    all_data = pd.DataFrame()

    # 获取文件夹中的所有CSV文件
    csv_files = [f for f in os.listdir(folder_path) if f.endswith('.csv')]

    # 逐个读取并合并CSV文件
    for file in csv_files:

        file_path = os.path.join(folder_path, file)
        print(f"开始合并CSV文件：{file}")
        data = pd.read_csv(file_path)
        all_data = pd.concat([all_data, data])

    return all_data

