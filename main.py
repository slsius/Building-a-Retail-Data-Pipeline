import pandas as pd
import os
import pathlib

# Start here...
def extract(sql_table, parquet_data):   #function combine two data
    merged_df = pd.merge(sql_table,parquet_data,left_index=False, right_index=False)
    return merged_df


def filldate(df):   #filling in data logically base on the previous date. if previous date not possible fill in the possible date
    if df['Date'].isnull().sum() > 0:
        df_na = df[df['Date'].isna()]
        df_drop = df.dropna(subset=['Date'])
        for index in df_na.index:
            if (index == 0): #if it is the first row
                df.loc[index, 'Date'] = df['Date'].min()
            elif(index == len(df) - 1): # if it is the last row
                df.loc[index, 'Date']= df['Date'].max()
            elif(df.loc[index-1,'Date'] == df.loc[index+1,'Date'] ): 
                #if previous and next row has the same date
                df.loc[index, 'Date'] = df.loc[index-1,'Date']
            else:
                df_filter = df_drop[
                    (df_drop['Store_ID'] == df.loc[index, 'Store_ID']) &                         
                    (df_drop['Dept'] == df.loc[index, 'Dept']) &
                    (df_drop.index > index-500)]    #find the last date
                #validate the date is not duplicated
                df_check = df_drop[
                    (df_drop['Store_ID'] == df.loc[index, 'Store_ID']) &                         
                    (df_drop['Dept'] == df.loc[index, 'Dept']) &
                    (df_drop['Date'] == (df_filter['Date'].max()) + pd.DateOffset(7))] 
                if(df_check.empty):
                    df.loc[index, 'Date'] = (df_filter['Date'].max()) + pd.DateOffset(7)   
    return df

def fillsales(df):  #fill in empty data with average sales value
    df_na = df[df['Weekly_Sales'].isna()]
    df_drop = df.dropna(subset=['Weekly_Sales'])
    for index in df_na.index:
        df_filter = df_drop[
                    (df_drop['Store_ID'] == df.loc[index, 'Store_ID']) &                         
                    (df_drop['Dept'] == df.loc[index, 'Dept']) &
                    (df_drop.index < index)]
        if(df_filter.empty):
            df_filter = df_drop[
                    (df_drop['Store_ID'] == df.loc[index, 'Store_ID']) &                         
                    (df_drop['Dept'] == df.loc[index, 'Dept']) &
                    (df_drop.index > index)&
                    (df_drop.index < index+500)]
            df.loc[index, 'Weekly_Sales'] = df_filter['Weekly_Sales'].mean()
        else:    
            df.loc[index, 'Weekly_Sales'] = df_filter['Weekly_Sales'].mean()
    return df

def transform(df):   #function to fill empty rolls and drop row which sales < 10000. also adding a month column which will use later
    df['CPI'] = df['CPI'].fillna(method='backfill') #fill CPI
    df['Unemployment'] = df["Unemployment"].fillna(method='backfill') #fill umeployment
    df = filldate(df) #fill in Null date
    df = fillsales(df) #fill in Null Sales
    df = df.drop(df[df['Weekly_Sales'] < 10000].index) #Drop column sales <10000
    
    df['Date'] = pd.to_datetime(df['Date'], format='%Y-%m-%dT%H:%M:%S.%f')  #change date format to datetime
    df['Month'] = df['Date'].dt.strftime('%m')   #create a Month column and store the month only
    
    df = df.loc[:, df.columns.intersection(['Store_ID','Month','Dept','IsHoliday','Weekly_Sales','CPI','Unemployment'])] 
    # drop unused col
    return df

def avg_monthly_sales(df):
    df['Avg_Sales'] = df['Weekly_Sales']
    agg_functions = {'Month':'first','Avg_Sales': 'mean'}
    df = df.groupby(['Month']).aggregate(agg_functions)
    df['Avg_Sales'] = df['Avg_Sales'].round(2)
    df["Month"] = pd.to_numeric(df["Month"])
    return df

def load(full_data, full_data_file_path, agg_data, agg_data_file_path):
  	# Save both DataFrames as csv files. Set index = False to drop the index columns
    full_data.to_csv(full_data_file_path, index = False)
    agg_data.to_csv(agg_data_file_path, index = False)

def validation(file_path):
  	# Use the "os" package to check whether a path exists
    file_exists = os.path.exists(file_path)
    # Raise an exception if the path doesn't exist, hence, if there is no file found on a given path
    if not file_exists:
        raise Exception(f"There is no file at the path {file_path}")
    
df = pd.read_parquet('extra_data.parquet')  #read parquet data
merged_df = extract(grocery_sales,df) #call function to combine data
clean_data = transform(merged_df) # Filling NA value and adding coloumn
agg_data = avg_monthly_sales(clean_data) #Getting Month and  Avg Sales

path = str(pathlib.Path().resolve()) # declare the path to save

load(clean_data, "clean_data.csv", agg_data, "agg_data.csv")
validation("clean_data.csv")
validation("agg_data.csv")