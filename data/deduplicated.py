import pandas as pd
from fuzzywuzzy import fuzz
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading

def deduplicate_value(row, deduplicated_values, threshold, lock):
    value = row['微博正文']
    publication_time = row['发布时间']
    with lock:
        for deduplicated_row in deduplicated_values:
            similarity = fuzz.ratio(value, deduplicated_row['微博正文'])
            if similarity >= threshold:
                return None
        return {'微博正文': value, '发布时间': publication_time}


def deduplicate_csv(input_file, output_file, column_name, threshold, num_workers=4):
    # Read CSV file
    df = pd.read_csv(input_file)

    # Extract unique values and publication times
    unique_values = df[column_name].unique()
    publication_times = df.groupby(column_name)['发布时间'].first().reset_index()

    # Create a list to store deduplicated rows
    deduplicated_values = []

    # Create a lock object
    lock = threading.Lock()

    # Use multithreading for parallel processing
    with tqdm(total=len(unique_values), desc="Deduplicating") as progress_bar, ThreadPoolExecutor(
            max_workers=num_workers) as executor:
        futures = []
        for i, value in enumerate(unique_values):
            row = {'微博正文': value, '发布时间': publication_times.loc[i, '发布时间']}
            future = executor.submit(deduplicate_value, row, deduplicated_values, threshold, lock)
            futures.append(future)

        for future in as_completed(futures):
            row = future.result()
            if row is not None:
                deduplicated_values.append(row)
            progress_bar.update(1)

    # Create a deduplicated DataFrame
    df_deduplicated = pd.DataFrame(deduplicated_values)

    # Save deduplicated results to a CSV file
    df_deduplicated.to_csv(output_file, index=False)

    print("Deduplicated results have been saved to", output_file)

