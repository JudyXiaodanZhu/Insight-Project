import pandas as pd # data processing, CSV file I/O (e.g. pd.read_csv)
from random import *
import numpy as np

#df = pd.read_csv('./mitbih_test.csv',header=None)
df = pd.read_csv("./output_ill.csv", header=0, index_col=0)
# df = pd.concat([df, df2], axis=0)
# df['My new column'] = 'default value'
# df.to_csv("output_sample.csv", index=False)
# df = pd.read_csv("./output_ill.csv", header=0, index_col=0)
filtered = df
sLen = len(filtered['1'])
filtered.info()
filtered.loc[:,'f'] = pd.Series(np.random.randint(2012,2425,sLen), index=filtered.index)
cols = filtered.columns.tolist()
cols = cols[-1:] + cols[:-1]
filtered = filtered[cols]
filtered.to_csv("output_sample.csv", index=False)
filtered.info()