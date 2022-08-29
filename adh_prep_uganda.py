# -*- coding: utf-8 -*-
"""
Created on Wed Aug 24 09:15:14 2022

@author: heiko
"""

import pandas as pd
from datetime import date, datetime, timedelta
import re


import pandas as pd
from datetime import date, datetime, timedelta

import tabula
import pandas as pd
import numpy as np
from datetime import date, datetime, timedelta
import glob
import re

base_data_path ='./data/uganda/'
files_list = glob.glob('%s*.xlsx'% base_data_path)
for i in range(len(files_list)):
    files_list[i] = files_list[i].replace("\\","/") 
   

logs = pd.read_csv('%sdata_log.txt'% base_data_path,header=None)
logs.columns=['done']
logs = logs.done.to_list()
files = pd.DataFrame()
files['files'] = files_list
file = files[~files.files.isin(logs)]
data_path = file.files.to_list()[0].split('.pdf')[0]

#%%

f = open('%sdata_log.txt'% base_data_path,'w')
for i in range(len(files_list)):
    f.write(files_list[i])
    f.write('\n')
f.close()

#%%

def get_last_date_of_month(year, month):
    """Return the last date of the month.
    
    Args:
        year (int): Year, i.e. 2022
        month (int): Month, i.e. 1 for January

    Returns:
        date (datetime): Last date of the current month
    """
    
    if month == 12:
        last_date = datetime(year, month, 31)
    else:
        last_date = datetime(year, month + 1, 1) + timedelta(days=-1)
    
    return last_date.strftime("%Y-%m-%d")

months = dict({'Jan':1,
               'Feb':2,
               'Mar':3,
               'Apr':4,
               'May':5,
               'Jun':6,
               'Jul':7,
               'Aug':8,
               'Sep':9,
               'Oct':10,
               'Nov':11,
               'Dec':12
               })
#%%
country = 'Uganda'
#data_path = './data/uganda/CPI_Composite_June_2022.xlsx'
month = [val for key, val in months.items() if key in data_path][0]
year = re.search(r'.*([1-3][0-9]{3})',data_path).group(1) # [1-3] = num between 1-3, [0-9]{3} = num 0-9 repeat 3 times
year = int(year)
last = get_last_date_of_month(year, month)
codes = pd.read_csv('./data/codeList.csv')
df = pd.read_excel(data_path,skiprows= 20, nrows= 14)
df = df.iloc[:,[1,-1]]
df.columns=['Indicator.Name',last]
df['Indicator.Name'] = df['Indicator.Name'].str.replace('Headline','All')
#df = df[df['Indicator.Name']!='Insurnace and Financial Services']
#%%
data_path= './data/imf/'
df_template = pd.read_csv('{}combined_imf_template.csv'.format(data_path))
df_template = df_template[df_template['Country']==country]
df_template = df_template.iloc[:,[0,1,2,3,4,-2,-1]]


#%%
# all items
def mapp_values(df,template):
    template = template.loc[:,['Indicator.Name','Indicator.Code']]
    values = ['All',
              'Food and non-',
              'Tobacco',
              'Clothing',
              'Communication',
              'Education',
              'Housing',
              'Household',
              'Health',
              'Miscellaneous',
              'Recreation',
              'Restaurants',
              'Transport',
              'Insurance']
        
    for i in range(len(values)):
        val = template[template['Indicator.Name'].str.contains(values[i],case=False)==True]
        try:
            df['Indicator.Name'][df['Indicator.Name'].str.contains(values[i],case=False)==True] = val['Indicator.Name'].values
        except:
            print('ERROR with: {}'.format(values[i]))    
    df = pd.merge(template,df,how='left',on = 'Indicator.Name')
    df = df.round(2)
    return df

df_1 = mapp_values(df,df_template)
df_1.to_csv('./outputs/uganda/{}_{}.csv'.format(country,last),index=False)

