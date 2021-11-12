import pandas as pd
import time
import sys
import requests
from pyarrow import parquet as pq
from pyarrow import Table as patable
import logging

#setup logging to send messages to terminal
logging.basicConfig(format='%(levelname)s - %(asctime)s - %(message)s', datefmt='%m/%d/%Y %I:%M:%S', level=logging.INFO)


def get_data(): 
  """Constructs API query to retrieve data from chosen buses. it returns a list of dicts and relies on \
    an access token and an access token secret in order to authenticate with the API"""

  api_base_url = "https://apitransporte.buenosaires.gob.ar/colectivos/vehiclePositionsSimple"

  api_url = api_base_url + '?client_id=' + access_token + '&client_secret=' + access_token_secret

  response = requests.get(api_url)


  lineas = ["159B","159E","159A", '152A', '61A', '62A', '93A', '93B', '117A', '117B',
    '101A', '101B', '4A', '4B', '4C', '23A', '46A', '46B', '47A', '47B']

  json_out = [i for i in response.json() if i["route_short_name"] in lineas]
  return json_out

def store_data(data, counter):
  """Coherces list of dicts to a tabular struct (df), sets it as a PyArrow table \
    and writes it as a parquet file in the out path  using counter value \
       to genuinelly identify each document"""
  
  pdf = pd.DataFrame(data)
  tableexp = patable.from_pandas(pdf)
  pq.write_table(tableexp, f"out/sample_{counter}.parquet")
  logging.info(f"written sample_{counter} with {len(data)} observations")





if __name__ == '__main__':
  logging.info("reading script")
  if len(sys.argv) != 4:
      logging.info('Usage: <access_token> <access_token_secret> <first_file_no>')
      sys.exit(1)
  logging.info('Correct number of args!')
  
  access_token = sys.argv[1]
  access_token_secret = sys.argv[2]
  counter = int(sys.argv[3])

  logging.info(f"access_token set as {access_token}\
    access_token_secret set as {access_token_secret}\
      counter set as {counter}")
  
  data = []
  while True:
    if len(data) >= 1000:
       store_data(data, counter)
       counter += 1
       data = []
    datum = get_data()
    data.extend(datum)
    logging.info(f"data has {len(data)} rows, setting system to sleep")
    time.sleep(240)



