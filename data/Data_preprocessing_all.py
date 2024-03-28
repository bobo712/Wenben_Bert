from Combined_csv import merge_csv_files
from deduplicated import deduplicate_csv
from deduplicated_all import deduplicate_csv_all



def dataperprocess_all(input_file,output_file_1,output_file_2,output_file_3):
    datacsv_all =merge_csv_files(input_file)
    datacsv_all.to_csv(output_file_1, index=False)
    print(f"CSV文件合并完成。保存在当前文件夹：{output_file_1}")
    #不需要去重清注释掉下面代码
    deduplicate_csv(output_file_1,output_file_2,"微博正文",90)
    deduplicate_csv_all(output_file_1,output_file_3,"微博正文",90)



if __name__ == "__main__":
    input_file=r"D:\wenben_Bert\Sentiment_analysis\年份情感分析"
    output_file_1="D:\wenben_Bert\Sentiment_analysis\总体情感分析/负面情绪.csv"
    output_file_2="data_Scipy/dataset_deduplicate.csv"
    output_file_3 = "data_Scipy/dataset_deduplicate_all.csv"
    dataperprocess_all(input_file,output_file_1,output_file_2,output_file_3)



