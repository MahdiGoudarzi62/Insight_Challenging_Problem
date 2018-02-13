import pandas as pd
from sqlalchemy import create_engine # database connection
import datetime as dt
#from IPython.display import display
import numpy as np

disk_engine = create_engine('sqlite:///itcont.db') # Initializes database with filename 311_8M.db in current directory


start = dt.datetime.now()
chunksize = 1
j = 0
index_start = 1
for df in pd.read_csv('insight_testsuite/tests/test_1/input/itcont.txt', chunksize=chunksize, iterator=True, encoding='utf-8',sep='|',
                     names=['V1','V2','V3','V4','V5','V6','V7','V8','V9','V10','V11','V12','V13','V14','V15','V16','V17','V18',
                           'V19','V20','V21']):
    
    df = df.rename(columns={c: c.replace(' ', '') for c in df.columns}) # Remove spaces from columns

    df['V14'] = pd.to_datetime(df['V14'],format='%m%d%Y', errors='coerce') # Convert to datetimes
        
    df.index += index_start

    # Remove the un-interesting columns
    columns = ['V1','V8','V11','V14','V15','V16']

    for c in df.columns:
        if c not in columns:
            df = df.drop(c, axis=1)    

    
    j+=1
    print ('{} seconds: completed {} rows'.format((dt.datetime.now() - start).seconds, j*chunksize))

    df.to_sql('data', disk_engine, if_exists='append')
    index_start = df.index[-1] +1
df = pd.read_sql_query('SELECT * FROM data  ', disk_engine)
df
from bloomfilter import BloomFilter # BloomFilter
df_final = pd.DataFrame(columns=['CMTE_ID','ZIP_CODE','YEAR','Percentile','Total_Amount','Number_of_Cont'])
#conversion of zip code to string
df['V11'] = df['V11'].astype(str)
df['V11'] = df['V11'].map(lambda x: x[:5])
#divide dataset to uniqe and repeat data
df_unique = pd.DataFrame(columns=df.columns)

df_repeat = pd.DataFrame(columns=df.columns)


total=0
# iteration over all row of main dataframe
for i,row in df.iterrows():
    if row['V16']!=None or len(row['V11'])<5 or row['V11']==None or row['V14']==None or row['V8']==None or row['V1']==None or row['V15']==None:
        continue
    elif df_unique.empty:
        df_unique=df_unique.append(row,ignore_index=False)
    else:
        word_name_absent = row['V8']
        word_zip_absent = row['V11']
    
        unique_name_present = list(df_unique['V8'])
        unique_zip_present = list(df_unique['V11'])
        n = len(unique_name_present) #no of items to add
        p = 0.05 #false positive probability
        bloomf_name = BloomFilter(n,p)
        bloomf_zip =BloomFilter(n,p)
        word_present_name = list(unique_name_present) # words to be added
        word_present_zip = list(unique_zip_present) # words to be added
        for item in word_present_name:
            bloomf_name.add(item)
        for item in word_present_zip:
            bloomf_zip.add(item)
        if bloomf_name.check(word_name_absent) and bloomf_zip.check(word_zip_absent):
            string = str(row['V14'])
            total=total + row['V15']
            df_repeat = df_repeat.append(row, ignore_index=False)
            df_final = df_final.append({'CMTE_ID':row['V1'],'ZIP_CODE':row['V11'],'YEAR':string[0:4],'Percentile':pd.Series(df_final['Total_Amount']).quantile(0.3,interpolation = 'nearest'),'Total_Amount':total,'Number_of_Cont':i-len(df_unique)}, ignore_index=True)
            df_final['Percentile'][0]=df_final['Total_Amount'][0]
        else:
            df_unique = df_unique.append(row)       

df_final
df_final.to_csv('repeat_donars.txt',sep='|',index=False,header=None, line_terminator=',')