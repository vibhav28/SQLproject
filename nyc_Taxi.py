import pandas as pd
import matplotlib.pyplot as plt
from sqlalchemy import create_engine, text
import numpy as np

df = pd.read_parquet("yellow_tripdata_2023-01.parquet")



#calling/connecting with mysql Database
mysql_engine = create_engine("mysql+pymysql://root:!v4i0b8h2@localhost/vibhavdb1")



##convert/ making sql table from parquet file 
df.to_sql("your_table", mysql_engine, if_exists="replace", index=False)

#printing no. of null values
print(df.isnull().sum())

df.replace([np.inf, -np.inf], np.nan, inplace=True)

#to print rows with null values
a = df.dropna(inplace=True)
print(df.columns.tolist())

#Convert datetime columns to proper datetime type
df['tpep_pickup_datetime'] = pd.to_datetime(df['tpep_pickup_datetime'], format='%Y/%m/%d %H:%M:%S')
df['tpep_dropoff_datetime'] = pd.to_datetime(df['tpep_dropoff_datetime'], format='%Y/%m/%d %H:%M:%S')
    
# Function to remove outliers in tripdistance and fare amount
def remove_outliers_iqr(df, column):
    Q1 = df[column].quantile(0.25)
    Q3 = df[column].quantile(0.75)
    IQR = Q3 - Q1
    return df[(df[column] >= Q1 - 1.5 * IQR) & (df[column] <= Q3 + 1.5 * IQR)]

df = remove_outliers_iqr(df, 'trip_distance')
df = remove_outliers_iqr(df, 'fare_amount')



#Create new features:
#● Trip duration in minutes
b=df['Trip_duration_min'] =(df['tpep_dropoff_datetime']-df['tpep_pickup_datetime']).dt.total_seconds()/60


#● Speed (miles per hour)
#first converting distance from miles to km
df['trip_distance']=(df['trip_distance']*1.60934)


#calculate speed in km/h
df['speed_of_taxi']=(df['trip_distance']/(df['Trip_duration_min']/60))



#● Time of day category (morning/afternoon/evening/night)
def time_of_Day(dt):
    hour=dt.hour
    if 5<= hour <12:
        return 'Morning'
    elif 12<= hour <17:
        return 'Afternoon'
    elif 17<= hour <21:
        return 'Evening'
    else:
        return 'Night'
df['time_of_day']=df['tpep_pickup_datetime'].apply(time_of_Day)

#● Is weekend (True/False)
df['is_weekend'] = df['tpep_pickup_datetime'].dt.dayofweek.isin([5, 6])

df.replace([np.inf, -np.inf], np.nan, inplace=True)

#to upload new columns in sql
df.to_sql('your_table', mysql_engine, if_exists='replace', index=False)


df.to_csv('taxi.csv', index=False)


df.to_excel('taxi.xlsx', index=False)


#to remove the data that is before 2022 and( 2023 if u dont want 2022 also)
#df = df[df['tpep_pickup_datetime'].dt.year >= 2022]
df = df[df['tpep_pickup_datetime'].dt.year >= 2023]



##to plot daily revenue
#first we find daily revenue
# Group by date and sum the total amount
daily_revenue = df.groupby('tpep_pickup_datetime')['total_amount'].sum().reset_index()


#Group by date and sum the total amount for daily revenue
daily_revenue = df.groupby('tpep_pickup_datetime')['total_amount'].sum().reset_index()

# Calculate the 7-day moving average
daily_revenue['7_day_avg'] = daily_revenue['total_amount'].rolling(window=7).mean()




# Plotting daily revenue and the moving average trendline
plt.figure(figsize=(10, 5))
plt.plot(daily_revenue['tpep_pickup_datetime'], daily_revenue['total_amount'], '+-', label='Daily Revenue')
plt.plot(daily_revenue['tpep_pickup_datetime'], daily_revenue['7_day_avg'], color='red', label='7-Day Moving Average', linewidth=2)
plt.xticks(rotation=45)
plt.title('Daily Revenue with 7-Day Moving Average')
plt.xlabel('Date')
plt.ylabel('Total Amount')
plt.legend()
plt.show()





