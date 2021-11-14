from pyarrow import parquet
import pandas as pd
import pyarrow
from datetime import datetime
from natsort import natsorted
from pyarrow import Table as patable
import glob

from geopy import distance

def build_batch(files):
    dataframes = []
    for file in files:
        dataframes.append(pyarrow.parquet.read_pandas(file).to_pandas())
    batch = pd.concat(dataframes)
    return batch

def process_batch(batch, counter):

    batch.sort_values('timestamp', ascending = False, inplace = True)
    batch['day'] = batch.timestamp.apply(lambda x: datetime.fromtimestamp(x).day)

    max_time = batch.groupby(['id', 'route_short_name', 'direction', 'route_id', 'day']).head(1)
    min_time = batch.groupby(['id', 'route_short_name', 'direction', 'route_id', 'day']).tail(1)

    merge_df = pd.merge(max_time, min_time , how = 'inner',
        on = ['id', 'route_short_name', 'direction', 'day'],
        suffixes= ('_max', '_min'))


    merge_df['dist'] =  [distance.distance((lat1,lon1), (lat2,lon2)).meters for lat1,lon1,lat2,lon2 in zip(merge_df.latitude_max, merge_df.longitude_max, merge_df.latitude_min, merge_df.longitude_min)]

    merge_df['seconds'] = merge_df.timestamp_max - merge_df.timestamp_min

    merge_df = merge_df.loc[merge_df.seconds > 0]

    merge_df = merge_df.groupby(['id','route_short_name', 'day']).agg({'dist': 'sum', 'seconds':'sum'}).reset_index() 

    parquet.write_table(patable.from_pandas(merge_df), 'out_transformed/transformed_' + str(counter) +'.parquet' )

    print('processed batch ' + str(counter))


def process_transformed(files):
    # Con la información ya generada, se pueden calcular las respuestas
    dataframe = build_batch(files)

    summ_df = dataframe.groupby(['id','route_short_name', 'day']).agg({'dist': 'sum', 'seconds':'sum'}).reset_index() 
    summ_df['speed_kmh'] = (summ_df.dist/1000) / (summ_df.seconds / 3600)

    


    print('Velocidad por día por línea')
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


    # Hay que agrupar por las dimensiones necesarias y aplicar funciones de agregación:
    # dataframe.groupby(['route_short_name','day']).seconds.sum()
    # dataframe.groupby(['route_short_name','day']).distance.sum()
    # Y así calcular la velocidad de cada bloque

    # Imprimir las respuestas para
    # - velocidad por día de cada línea
    # - interno más rápido de cada línea

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