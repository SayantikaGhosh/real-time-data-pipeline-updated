
# import pandas as pd
# df = pd.read_parquet('./deltalake/gold/user_metrics')
# df2 = pd.read_parquet('./deltalake/gold/order_metrics')
# print(df.shape)
# # print(df.sort_values("window_end").tail(5))
# print(df2.shape)
# print(df.columns)
# print(df2.columns)

import pandas as pd
df = pd.read_parquet('./deltalake/gold/order_metrics/part-00000-b0d5e159-16ac-40fe-a35f-d4b15a70b216-c000.snappy.parquet')
print(df.shape)
print(df.head())

