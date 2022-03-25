# data_manipulator.py
import os
import logging

import pandas as pd
import numpy as np

from dateutil.parser        import parse
from dateutil.relativedelta import relativedelta

from tqdm import tqdm

def get_input_data(bucket='esp-big-data', initial_directory='BigData', step = 'raw', filename='datos-abiertos-agosto-2019.csv'):
    """Read a csv file in a bucket of GCS, the file must use latin1 encoding and the separator is a semicolon (;)

    Args:
        bucket (str, optional): Name of the bucket. Defaults to 'esp-big-data'.
        initial_directory (str, optional): project directory. Defaults to 'BigData'.
        filename (str, optional):csv file to read. Defaults to 'datos-abiertos-agosto-2019.csv'.

    Returns:
        pandas.dataframe: dataframe with the raw data
    """

    logger = logging.getLogger('get_input_data')

    # get the path in a bucket in GCS
    project_dir = 'gs://{bucket_name}/{directory}'.format(
        bucket_name = bucket,
        directory   = initial_directory
    )

    # join the path to have the full path of the file
    filepath = os.path.join(project_dir, 'data', step)
    file     = os.path.join(filepath, filename)

    logger.info('Reading file: {}'.format(file))
    data = pd.read_csv(file, encoding='latin1', sep=';')
    logger.info('Done!!')
 
    return data


def clean_datetime_cols(df, initial_time_col = 'FECHA_INCIDENTE', final_time_col='FECHA_INICIO_DESPLAZAMIENTO_MOVIL'):
    
    df_date_nan   = df[df[initial_time_col].isna()]
    df_date_nonan = df[~df[initial_time_col].isna()]
    
    
    ti = df_date_nonan[initial_time_col].apply(parse)
    tf = df_date_nonan[final_time_col].apply(parse)

    deltat = tf - ti
    list_minutes = list()
    for delta_time in deltat.values:
        seconds = int(delta_time)/1e9
        list_minutes.append(seconds/60.)
    
    
    list_new_dates = list()
    for time in tqdm(df_date_nan[final_time_col].values):
        new_date = parse(time) - relativedelta(minutes=np.median(list_minutes))
        list_new_dates.append(new_date)
        
    df_date_nan[initial_time_col] = list_new_dates
    df_out = pd.concat([df_date_nan, df_date_nonan])
    
    # df_out[initial_time_col] = df_out[initial_time_col].apply(lambda x: parse(x)) 
    # df_out[final_time_col]   = df_out[final_time_col].apply(lambda x: parse(x))
    
    return df_out


if __name__ == '__main__':
    # set up the logging output format (to visualize in a terminal)
    log_fmt = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    logging.captureWarnings(True)
    logging.basicConfig(
        level  = logging.INFO,
        format = log_fmt
    )

    get_input_data()