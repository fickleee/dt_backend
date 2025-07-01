from datetime import datetime, timedelta
import pandas as pd
import numpy as np
from sklearn.linear_model import LinearRegression

start_hour = 9
end_hour = 15

def compute_degradation_score(string_id, end_date, time_window, current_df, rad_df):
    value_list = []
    for i in range(3):
        # previous_year = 0 # -------------- need to revise
        previous_year = i
        # Convert end_date to datetime if it's a string
        if isinstance(end_date, str):
            end_date = datetime.strptime(end_date, '%Y-%m-%d')
        # print(end_date) 
        end_date_i = end_date - timedelta(days=previous_year*365 + 1)
        end_time = end_date_i.replace(hour=23, minute=59, second=59)
        start_date_i = end_date - timedelta(days=previous_year*365 + time_window)
        start_time = start_date_i.replace(hour=0, minute=0, second=0, microsecond=0)
        
        new_current_df = current_df[current_df['device_id'] == string_id]
        
        if(len(new_current_df) == 0):
            return 0
        
        new_current_df = new_current_df[(current_df['time'] >= start_time) & (current_df['time'] <= end_time)]
        new_rad_df = rad_df[(rad_df['time'] >= start_time) & (rad_df['time'] <= end_time)]

        current_rad_df = new_current_df.merge(new_rad_df, on='time', how='inner')
         
        rad_col = 'irradiance'

        current_rad_df = current_rad_df[[rad_col, 'intensity', 'time']]
        current_rad_df = current_rad_df[(current_rad_df['time'].dt.hour >= start_hour) & (current_rad_df['time'].dt.hour <= end_hour)]

        rad_current_df = current_rad_df.drop(columns=['time'])

        rad_current_df = head_tail_filter(rad_current_df)
        value = linear_fit(rad_current_df)
        value_list.append(value)
    
    degradation_score = calc_degradation_score(value_list)
    return degradation_score



def transform_rad(data):
    # # data is one column of dataframe, such as df[column1]
    data = np.log1p(data)  # log1p handles zero values
    data = np.power(data, 6)
    return data

def transform_intensity(data):
    data = data
    return data

def head_tail_filter(df):
    # there are two columns a and b in the df
    # retain rows between the 0.1*max(a) and 0.9*max(a)
    # retain rows between the 0.1*max(b) and 0.9*max(b)
    # return the filtered df
    max_a = df.iloc[:,0].max()
    max_b = df.iloc[:,1].max()
    df = df[(df.iloc[:,0] > 0) & (df.iloc[:,0] <= 0.98*max_a)]
    df = df[(df.iloc[:,1] > 0) & (df.iloc[:,1] <= 0.98*max_b)]
    return df

def linear_fit(rad_current_df):
    # if df is empty, return 0
    if rad_current_df.empty:
        return 0
    # print(rad_current_df.columns)
    # Fit linear regression model
    X = transform_rad(rad_current_df.iloc[:, 0].values.reshape(-1, 1))  # First column as X
    y = transform_intensity(rad_current_df.iloc[:, 1].values.reshape(-1, 1))  # Second column as y
    
    model = LinearRegression()
    model.fit(X, y)
    
    # Return slope (a) from y = ax + b
    return model.coef_[0][0]

def calc_degradation_score(set):
    # if one of the number is 0 or smaller than 0, return 0
    if set[-1] <=0:
        return 0
    if set[0] <=0:
        if set[1] <=0:
            return 0
        else:
            return (set[1] - set[-1]) / set[1] if (set[1] - set[-1]) / set[1] > 0 else 0
        
    if (set[0] - set[-1]) / set[0] >=0.5:
        return 0
    
    return (set[0] - set[-1]) / set[0] if (set[0] - set[-1]) / set[0] > 0 else 0






