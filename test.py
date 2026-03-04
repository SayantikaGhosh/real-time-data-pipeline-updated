
import pandas as pd
df = pd.read_parquet('./deltalake/gold/user_metrics')
df2 = pd.read_parquet('./deltalake/gold/order_metrics')
print(df.shape)
# print(df.sort_values("window_end").tail(5))
print(df2.shape)
# print(df.head(5))
# print(df2.head(5))


