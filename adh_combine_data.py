# -*- coding: utf-8 -*-
"""
Created on Mon Aug 29 13:09:51 2022

We must assume that the ckan dataset is our prime dataset, and that all other data is being added to this

@author: heiko
"""

import pandas as pd
from datetime import date, datetime, timedelta


data_path_temp= './data/imf/'
df_ckan = pd.read_csv('{}combined_imf_template.csv'.format(data_path_temp))
cols = df_ckan.columns.to_list()
rem = cols[-2:]
cols = cols[0:5] + rem
df_temp = df_ckan[cols]
df_ckan = df_ckan.drop(columns=rem)

#%% get IMF data
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

df_imf = pd.read_excel('./outputs/imf/CPI_08-18-2022 14-17-37-67_timeSeries_africa.xlsx')
cols = df_imf.columns.to_list()
res = cols[4:]
cols = cols[0:4]
res = [get_last_date_of_month(int(val.split('M')[0]),int(val.split('M')[1])) for val in res]
cols = cols + res
df_imf.columns = cols
df_imf = df_imf.round(2)

cols = ['Country','Indicator.Name','Indicator.Code']+rem
df_imf_latest = df_imf.loc[:,cols]

#%% upate ckan with IMF latest

df_ckan = pd.merge(df_ckan,df_imf_latest,on=['Country','Indicator.Name','Indicator.Code'],how='left')

#%% find which individual country datasets we need and get individually scraped countries
countries = ['South Africa','Ethiopia','Kenya','Tanzania','Uganda']

data_paths = dict({'South Africa':'./outputs/south_africa/south_africa_',
               'Ethiopia':'./outputs/ethiopia/Ethiopia_',
               'Kenya':'./outputs/kenya/Kenya_',
               'Tanzania':'./outputs/tanzania/Tanzania_',
               'Uganda':'./outputs/uganda/Uganda_'})
#%%

def update_ckan(df_ckan,file):
    df = pd.read_csv('{}.csv'.format(file))
    df['Country'] = countries[i]
    df = df.set_index(['Country','Indicator.Name','Indicator.Code'])
    df_ckan = df_ckan.set_index(['Country','Indicator.Name','Indicator.Code'])
    df_ckan.update(df)
    df_ckan = df_ckan.reset_index()
    return df_ckan

for i in range(0,len(countries)):
    
    df_ckan_country = df_ckan[df_ckan.Country == countries[i]]
    
    if df_ckan_country[rem[0]].isnull().all():
        #print("need data for {} {}".format(countries[i],rem[0]))
        file = data_paths[countries[i]]+rem[0]
       
        try:
            df_ckan = update_ckan(df_ckan,file)
        except:
            print('failed to update {} {}'.format(countries[i],rem[0]))
            
    if df_ckan_country[rem[1]].isnull().all():
        #print("need data for {} {}".format(countries[i],rem[1]))
        file = data_paths[countries[i]]+rem[1]
        try:
            df_ckan = update_ckan(df_ckan,file)
        except:
            print('failed to update {} {}'.format(countries[i],rem[1]))
            

df_ckan.to_csv('./outputs/ckan/{}_combined_imf_database.csv'.format(rem[-1]),index=False)

#%% clean check

#del df_ckan, df_eth, df_imf, df_imf_latest, df_imf_c, df_temp_c, df_orig, df_ken, df_tan, df_ug, df_sa, df

data_path_temp= './data/imf/'
df_old = pd.read_csv('{}combined_imf_template.csv'.format(data_path_temp))
df_new = pd.read_csv('./outputs/ckan/{}_combined_imf_database.csv'.format(rem[-1]))

def compare(df1,df2,country):
    df1 = df1[df1.Country==country]
    df2 = df2[df2.Country==country]
    
    return df1.iloc[:,[2,3,4,-5,-4,-3,-2,-1]],df2.iloc[:,[0,1,2,-5,-4,-3,-2,-1]]

i = 3
country = countries[i]
df_old_c, df_new_c = compare(df_old,df_new,country)


