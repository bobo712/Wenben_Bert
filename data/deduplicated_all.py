import pandas as pd
from fuzzywuzzy import fuzz
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading


def deduplicate_row(row, deduplicated_values, column_name, threshold, lock):
    with lock:
        is_duplicate = False
        for value in deduplicated_values:
            similarity = fuzz.ratio(row[column_name], value[column_name])
            if similarity >= threshold:
                is_duplicate = True
                break

        if not is_duplicate:
            deduplicated_values.append(row)


def deduplicate_csv_all(input_file, output_file, column_name, threshold, num_workers=4):
    # 读取CSV文件
    df = pd.read_csv(input_file)

    # 用于保存去重结果的列表
    deduplicated_values = []

    # 创建锁对象
    lock = threading.Lock()

    # 使用多线程进行并行处理
    with tqdm(total=len(df), desc="Deduplicating") as progress_bar, ThreadPoolExecutor(
            max_workers=num_workers) as executor:
        futures = []
        for _, row in df.iterrows():
            future = executor.submit(deduplicate_row, row, deduplicated_values, column_name, threshold, lock)
            futures.append(future)

        for future in as_completed(futures):
            progress_bar.update(1)

    # 创建去重后的DataFrame
    df_deduplicated = pd.DataFrame(deduplicated_values)

    # 保存去重结果为CSV文件
    df_deduplicated.to_csv(output_file, index=False)
    print("去重结果已保存到", output_file)
