import pandas as pd
import numpy as np

pd.set_option('display.max_rows', 100)
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)
#df = pd.read_csv("/Users/Shared/data/test/TEST_TABLE0.csv",low_memory=False,names=['DIREPL_PACKAGEID','DIRRPL_PID', 'DIREPL_STATUS','DIREPL_UPDATED','INDEX','INT_NUM'])
df = pd.read_csv("/Users/Shared/data/test/TEST_TABLE0.csv",low_memory=False)
print(df.head(20))
print('Shape: {} - {}'.format(df.shape[0],df.shape[1]))

#dupdf = df[df.duplicated(subset=['INDEX'],keep=False)].sort_values(by='INDEX')

#print(dupdf)
