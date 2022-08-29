# -*- coding: utf-8 -*-
"""
Created on Tue Aug 23 13:42:55 2022
map codes from countries to IMF
@author: heiko
"""
import pandas as pd
from datetime import date, datetime, timedelta

url = 'https://raw.githubusercontent.com/adamoxford/InflationData/master/codeList.csv'
codes = pd.read_csv(url)


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



#%%
def get_data(filename,last,col):
    df_1 = pd.read_csv(filename)
    df_1 = pd.read_csv('{}'.format(data_path))
    
    df_1 = df_1.rename(columns={'divisions':'Indicator.Name',col:last})
    df_1 = df_1.loc[:,['Indicator.Name',last]]
    return df_1

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

countries = dict({'South Africa':['../data/south_africa/P0141June2022_Tables.csv','Percentage change'],
                  'Kenya':['../data/kenya/July-2022-CPI-.csv','% Change on same\rmonth of the\rprevious year (July\r2022/ July 2021)'],
                  'Ethiopia':['../data/ethiopia/CPI_July_2022.csv','percentage_change']})



#data_path = '../data/south_africa/P0141June2022_Tables.csv'
country_list = ['South Africa','Kenya','Ethiopia']
country = country_list[2]
data_path = countries[country][0]
col = countries[country][1]
year = 2022
month = [val for key, val in months.items() if key in data_path][0]
#month = months[month]
last = get_last_date_of_month(year, month)
df_1 = get_data(data_path,last,col)
df_orig = get_data(data_path,last,col)
#%%
data_path= '../data/imf/'
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
    #i = 2    
        val = template[template['Indicator.Name'].str.contains(values[i],case=False)==True]
    
        df['Indicator.Name'][df['Indicator.Name'].str.contains(values[i],case=False)==True] = val['Indicator.Name'].values
    
    df = pd.merge(template,df,how='left',on = 'Indicator.Name')
    return df

df_1 = mapp_values(df_1,df_template)


#dat = calendar.monthrange(2022, 6)
#%%
df_result = df_template.drop(columns=last)
df_result = pd.merge(df_result,df_1,how='left',on=['Indicator.Name','Indicator.Code'])




