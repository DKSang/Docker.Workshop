import sys
import pandas as pd
print("argument",sys.argv)
month=int(sys.argv[1])
df=pd.DataFrame({"day":[1,2],"num_passenger":[10,15]})
df["month"]=month
df.to_parquet(f"output_{month}.parquet")
print(df.head())
