import boto3
import pandas as pd
from io import StringIO,BytesIO
from datetime import datetime, timedelta


# Adaptor_Function Parts
# read from s3
def read_csv_to_df(bucket,key):
    csv_obj = bucket.Object(key=key).get().get('Body').read().decode('utf-8')
    data = StringIO(csv_obj)
    df = pd.read_csv(data, delimiter=',')
    return df

def write_to_s3(bucket,key,df):
    out_buffer = BytesIO()
    df.to_parquet(out_buffer, index=False)
    bucket.put_object(Body=out_buffer.getvalue(), Key=key)
    return True

def write_to_s3_csv(bucket,key,df):
    out_buffer = StringIO()
    df.to_csv(out_buffer, index=False)
    bucket.put_object(Body=out_buffer.getvalue(), Key=key)
    return True

def parquet_to_df(bucket,key):
    s3 = boto3.resource('s3')
    bucket_target = s3.Bucket(bucket)
    parquet_obj = bucket_target.Object(key=key).get().get('Body').read()
    data = BytesIO(parquet_obj)
    df = pd.read_parquet(data)
    return df

# App_Function Parts


#Extract
def extract(bucket,objects):
    df = pd.concat([read_csv_to_df(bucket,obj.key) for obj in objects], ignore_index=True)
    return df
#Transform

def transform (df, columns, arg_date):
    # groupby section
    df = df.loc[:, columns]
    df.dropna(inplace=True)
    df['opening_price'] = df.sort_values(by=['Time']).groupby(['ISIN', 'Date'])['StartPrice'].transform('first')
    df['closing_price']=df.sort_values(by=['Time']).groupby(['ISIN', 'Date'])['EndPrice'].transform('last')

    #aggregation
    df = df.groupby(['ISIN', 'Date'], as_index=False).agg(opening_price_eur=('opening_price', 'min'), closing_price_eur=('closing_price', 'min'), minimum_price_eur=('MinPrice', 'min'), maximum_price_eur=('MaxPrice', 'max'), daily_traded_volume=('TradedVolume', 'sum'))
    df['prev_closing_price'] = df.sort_values(by=['Date']).groupby(['ISIN'])['closing_price_eur'].shift(-1)
    return df

#Load
def load(bucket,key,df):
    write_to_s3(bucket,key,df)
    # update_meta_file(bucket,key,min_date)

#report
def etl_report1(src_bucket,trg_bucket,objects,key_target,columns,arg_date):
    df= extract(src_bucket,objects)
    df= transform(df, columns, arg_date)
    load(trg_bucket,key_target,df)

#reading list
def missing_date_list(bucket,key,arg_date,today_str,src_format):
    start= datetime.strptime(arg_date,src_format).date()-timedelta(days=1)
    today= datetime.strptime(today_str,src_format).date()
    df_meta = read_csv_to_df(bucket,key)
    dates=[start+ timedelta(days= x) for x in range(0, (today - start).days + 1)]
    src_dates = set(pd.to_datetime(df_meta['source_date']).dt.date)
    dates_missing = set(dates[1:]) - src_dates
    min_dates=min(set(dates[1:]) - src_dates)
    min_date= min_dates.strftime(src_format)
    return min_dates

def update_meta_file(bucket,key,min_date):
    #read csv
    df_new=pd.DataFrame(columns=['source_date', 'datetime_of_processing'])
    df_new['source_date'] =''
    df_new['datetime_of_processing'] = datetime.today().strftime('%Y-%m-%d')
    df_old=read_csv_to_df(bucket,key)
    df_all=pd.concat([df_old, df_new])

    #write csv
    write_to_s3_csv(bucket,key,df_all)





def main():
    #  initial parameters
    arg_date = '2021-04-22'
    today_str = '2021-04-25'
    src_format='%Y-%m-%d'
    src_bucket='deutsche-boerse-xetra-pds'
    trg_bucket='python-xetra-1234'
    # arg_date_dt=datetime.strptime(arg_date,src_format).date()-timedelta(days=1)
    key_target= 'xetra_daily_report_' + datetime.today().strftime("%Y%m%d_%H%M%S") + '.parquet'
    columns = ['ISIN', 'Date', 'Time', 'StartPrice', 'MaxPrice', 'MinPrice', 'EndPrice', 'TradedVolume']
    meta_key='meta_file.csv'

    s3 = boto3.resource('s3')
    bucket= s3.Bucket(src_bucket)
    bucket_end= s3.Bucket(trg_bucket)
    

    min_date= missing_date_list(bucket_end,meta_key,arg_date,today_str,src_format)
    objects=[obj for obj in bucket.objects.all() if datetime.strptime(obj.key.split('/')[0],src_format).date()== min_date]

    etl_report1 (bucket,bucket_end,objects,key_target,columns,arg_date)

    #df_report = parquet_to_df(trg_bucket,'xetra_daily_report_20210821_191218.parquet')
    #print(df_report)


main()


