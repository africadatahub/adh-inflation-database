# -*- coding: utf-8 -*-
"""
Created on Wed Aug 24 10:46:41 2022

@author: heiko
"""

import tabula
import pandas as pd
import numpy as np
from datetime import date, datetime, timedelta
import glob
import re

bae_data_path ='./data/ethiopia/'
files_list = glob.glob('%s*.pdf'% bae_data_path)
for i in range(len(files_list)):
    files_list[i] = files_list[i].replace("\\","/") 
   
logs = pd.read_csv('%sdata_log.txt'% bae_data_path,header=None)
logs.columns=['done']
logs = logs.done.to_list()
files = pd.DataFrame()
files['files'] = files_list
file = files[~files.files.isin(logs)]


f = open('%sdata_log.txt'% bae_data_path,'w')
for i in range(len(files_list)):
    f.write(files_list[i])
    f.write('\n')
f.close()

specify = False
if specify == True:
    data_path = './data/ethiopia/CPI_June_2022.pdf'
    #data_path = './data/ethiopia/CPI_July_2022.pdf'
else:
    data_path = file.files.to_list()[0]

#%%
tables = tabula.read_pdf("{}".format(data_path), pages=23, stream=True, area=(175.725,146.19,416.295,739.2))
df_eth = tables[0]
df_eth = df_eth.drop(columns=['Non-Food Index','Unnamed: 0','Month an Year'])
df_eth = pd.DataFrame(np.vstack([df_eth.columns, df_eth]))


df_eth_labels = df_eth.head(6)
df_eth_labels = df_eth_labels.fillna('')
df_eth_labels = df_eth_labels.apply(' '.join).reset_index(drop=True)
df_eth_labels.iloc[7] = df_eth_labels.iloc[7].replace(' ','')
df_eth_labels = df_eth_labels.str.replace('General Index','All items')
df_eth_labels = df_eth_labels.str.replace('Rstaurant','Restaurant')
df_eth_labels = df_eth_labels.str.replace('Misellanous','Miscellaneous')
df_eth_labels = df_eth_labels.str.replace('ousehold','household')

df_eth_data = df_eth.iloc[-1,:]

df = pd.concat([df_eth_labels,df_eth_data],axis=1)

# unscramble
df_s = df.iloc[4,:]
df_s = df_s.to_frame().transpose()

df_s['Housing, Water, Electricity, Gas and Other Fuels'] = df_s.iloc[0,1].split(' ')[0]
df_s['Furnishings,Household Equipment and Routine Maintenance'] = df_s.iloc[0,1].split(' ')[1]
df_s = df_s[['Housing, Water, Electricity, Gas and Other Fuels','Furnishings,Household Equipment and Routine Maintenance']]

df_s = df_s.transpose()
df_s = df_s.reset_index()
df_s.columns=['divisions','percentage_change']

df = df.drop([4])
df.columns=['divisions','percentage_change']
df = pd.concat([df,df_s])
df.percentage_change = df.percentage_change.astype(float)

df.to_csv('{}.csv'.format(data_path.split('.pdf')[0]),index=False)

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
country = 'Ethiopia'
month = [val for key, val in months.items() if key in data_path][0]
year = re.search(r'.*([1-3][0-9]{3})',data_path).group(1) # [1-3] = num between 1-3, [0-9]{3} = num 0-9 repeat 3 times
year = int(year)
last = get_last_date_of_month(year, month)
codes = pd.read_csv('./data/codeList.csv')
df = df.iloc[:,[0,-1]]
df.columns = ['Indicator.Name',last]
df[last] = df[last].astype(float)
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

df_1.to_csv('./outputs/{}/{}_{}.csv'.format(country.lower(),country,last),index=False)

#%%




