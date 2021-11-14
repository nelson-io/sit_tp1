from pyarrow import parquet
import pandas as pd
import pyarrow
from datetime import datetime
from natsort import natsorted
from pyarrow import Table as patable
import glob
from geopy import distance

def build_batch(files):
    """Reads and concats multiple parquet files into a dataframe and returns it"""
    dataframes = []
    for file in files:
        dataframes.append(pyarrow.parquet.read_pandas(file).to_pandas())
    batch = pd.concat(dataframes)
    return batch

def process_batch(batch, counter):
    """Processes a batch dataframe, estimating the distance and time elapsed \
        between first and last observation per group"""

    # Since provided snippets didn't work, it sort values by timestamp in order 
    # to extract max and min values per group
    batch.sort_values('timestamp', ascending = False, inplace = True)

    # Add day column to DF
    batch['day'] = batch.timestamp.apply(lambda x: datetime.fromtimestamp(x).day)

    # Generate DFs with first and last obs per group only
    max_time = batch.groupby(['id', 'route_short_name', 'direction', 'route_id', 'day']).head(1)
    min_time = batch.groupby(['id', 'route_short_name', 'direction', 'route_id', 'day']).tail(1)

    # Merge DFs in order to associate the first API response from the batch
    # to the last one in a tabular format
    merge_df = pd.merge(max_time, min_time , how = 'inner',
        on = ['id', 'route_short_name', 'direction', 'day'],
        suffixes= ('_max', '_min'))

    # Estimates distance between first and last observation
    merge_df['dist'] =  [distance.distance((lat1,lon1), (lat2,lon2)).meters for lat1,lon1,lat2,lon2 in zip(merge_df.latitude_max, merge_df.longitude_max, merge_df.latitude_min, merge_df.longitude_min)]

    # Estimates time elapsed between first and last observation
    merge_df['seconds'] = merge_df.timestamp_max - merge_df.timestamp_min

    # Remove cases with no time elapsed
    merge_df = merge_df.loc[merge_df.seconds > 0]

    # summarise data in order to remove direction from grouping variables
    merge_df = merge_df.groupby(['id','route_short_name', 'day']).agg({'dist': 'sum', 'seconds':'sum'}).reset_index() 

    # Writes parquet
    parquet.write_table(patable.from_pandas(merge_df), 'out_transformed/transformed_' + str(counter) +'.parquet' )

    print('processed batch ' + str(counter))


def process_transformed(files):
    """it processes every transformed table written in parquets, generating multiple wranglings /
    to respond each question"""

    # imports parquets into a single DF
    dataframe = build_batch(files)

    #summarise the DF and estimate the 'approx' speed 
    summ_df = dataframe.groupby(['id','route_short_name', 'day']).agg({'dist': 'sum', 'seconds':'sum'}).reset_index() 
    summ_df['speed_kmh'] = (summ_df.dist/1000) / (summ_df.seconds / 3600)

    
    # Generate and print responses

    print('speed by route_short_name ')
    res_0 = summ_df.groupby(['route_short_name','day']).agg({'speed_kmh':'max'}).reset_index
    print(res_0)

    print('Interno más rápido por cada línea')
    res_1 = summ_df.sort_values('speed_kmh',ascending=False).groupby(['route_short_name']).head(1).reset_index().loc[:,['id','route_short_name', 'speed_kmh']]
    print(res_1)

    print('Interno más lento por cada línea')
    res_2 = summ_df.sort_values('speed_kmh',ascending=False).groupby(['route_short_name']).tail(1).reset_index().loc[:,['id','route_short_name', 'speed_kmh']]
    print(res_2)

    print('Velocidad promedio por linea')
    res_3 = summ_df.groupby('route_short_name').agg({'speed_kmh':'mean'})
    print(res_3)   

    print('dia max speed por linea')
    res_4 = summ_df.sort_values('speed_kmh',ascending=False).groupby(['route_short_name']).head(1).reset_index().loc[:,['route_short_name', 'day']]
    print(res_4)

    print('dia min speed por linea')
    res_5 = summ_df.sort_values('speed_kmh',ascending=False).groupby(['route_short_name']).tail(1).reset_index().loc[:,['route_short_name', 'day']]
    print(res_5)


if __name__ == '__main__':
    input_path = 'out/*.parquet'
    all_files = natsorted(glob.glob(input_path))
    chunk_size = 30
    chunks = [all_files[i:i + chunk_size] for i in range(0, len(all_files), chunk_size)]
    
    counter = 0
    for chunk in chunks:
        batch = build_batch(chunk)
        process_batch(batch, counter)
        counter += 1

    input_path = 'out_transformed/transformed_*.parquet'
    all_files = natsorted(glob.glob(input_path))
    process_transformed(all_files)