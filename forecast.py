import requests
import json
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

# paramaterising url

def api_request(latitude, longitude):

    URL = f'https://api.open-meteo.com/v1/forecast?latitude={latitude}&longitude={longitude}&hourly=temperature_2m,apparent_temperature,precipitation_probability,precipitation,snowfall,snow_depth&daily=sunrise&timezone=America%2FLos_Angeles'

    # checking api response

    response = requests.get(URL)

    if response.status_code >= 200 and response.status_code < 300:
        data = response.json()
        return data
    else:
        raise requests.exceptions.HTTPError(f"Received {response.status_code} status code from API")


def snow_report(latitude, longitude):

    data = api_request(latitude, longitude)
    hourly_data = data["hourly"]
    time = hourly_data['time']


    snowfall = hourly_data['snowfall']

    return time, snowfall

    # snow_time_dictionary = {}


    # print(snowfall)

    # snow_time_dictionary = {time[i]: snowfall[i] for i in range(len(time))}

    # # Printing resultant dictionary
    # return snow_time_dictionary

    # for snowinhour in snowfall:
    #     print(snowinhour)


def store_data_in_parquet_file(latitude, longitude):

    snow_time_dictionary = snow_report(latitude, longitude)

    # convert dictionary to pandas dataframe
    snowfall_data_file = pd.DataFrame.from_dict(snow_time_dictionary, orient="index", columns=["snowfall"])

    print(snowfall_data_file)

    # add a new column with the time as datetime
    snowfall_data_file.index = pd.to_datetime(snowfall_data_file.index)

    # reset index to make time a separate column
    snowfall_data_file = snowfall_data_file.reset_index()

    # rename index column to 'time'
    snowfall_data_file = snowfall_data_file.rename(columns={'index': 'time'})

    # store dataframe as Parquet file
    table = pa.Table.from_pandas(snowfall_data_file)
    pq.write_table(table, 'snowfall_data.parquet')

    # read the Parquet file into a pyarrow.Table object
    table = pq.read_table('snowfall_data.parquet')

    # convert the table to a pandas dataframe
    snowfall_data_file = table.to_pandas()

    # print the dataframe to see its contents
    # print(snowfall_data_file.head())




store_data_in_parquet_file(50.9584, 118.1631)
