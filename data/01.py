import pandas as pd

# 读取微博正文的CSV文件
df_weibo = pd.read_csv('D:\wenben_Bert\Sentiment_analysis\总体情感分析\负面情绪.csv')

# 读取另一个CSV文件
df_another = pd.read_csv('D:\wenben_Bert\Data_analysis\year\matching_dates_2020.csv')

# 获取微博正文列和另一个文件的目标列
weibo_texts = df_weibo['Text']
target_texts = df_another['微博正文']



# 存储匹配的文本和对应的行号
matched_texts = []
matched_rows_weibo = []
matched_rows_another = []

# 进行完全匹配并记录行号
for i, weibo_text in enumerate(weibo_texts):
    for j, target_text in enumerate(target_texts):
        if str(weibo_text) == str(target_text):
            matched_texts.append(target_text)
            matched_rows_weibo.append(i)
            matched_rows_another.append(j)

match_count = len(matched_texts)
print(f"另一个文件中包含微博正文的条数为：{match_count}")

# 保存匹配的文本和行号到文件
result_df = pd.DataFrame({
    '匹配的文本': matched_texts,
    '微博正文行号': matched_rows_weibo,
    '另一个文件行号': matched_rows_another
})
result_df.to_csv('result.csv', index=False)
