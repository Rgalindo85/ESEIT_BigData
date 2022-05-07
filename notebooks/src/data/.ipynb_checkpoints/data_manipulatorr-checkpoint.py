# data_manipulator.py
import os
import logging

import pandas as pd
import numpy as np

from dateutil.parser    import parse
from dateutil.relativedelta import relativedelta

from tqdm import tqdm

def get_input_data(bucket='eseit_bigdata', initial_directory='bigdata', filename='datos-abiertos-agosto-2019.csv', step='raw'):
    """Read a csv file in a bucket of GCS, the file must use latin1 encoding and the separator is a semicolon (;)

    Args:
        bucket (str, optional): Name of the bucket. Defaults to 'eseit_bigdata'.
        initial_directory (str, optional): project directory. Defaults to 'bigdata'.
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
    filepath = os.path.join(project_dir, 'DATA', step)
    file     = os.path.join(filepath, filename)

    logger.info('Reading file: {}'.format(file))
    data = pd.read_csv(file, encoding='latin1', sep=';')
    logger.info('Done!!')
 
    return data


def get_headers_from_metadata(bucket='eseit_bigdata', directory='bigdata', filename='metadatos-llamadas-urg-y-emer.csv'):
    """Read a csv file in a bucket of GCS, the file must use latin1 encoding and the separator is a semicolon (;)

    Args:
        bucket (str, optional): Name of the bucket. Defaults to 'eseit_bigdata'.
        directory (str, optional): project directory. Defaults to 'bigdata'.
        filename (str, optional):csv file to read. Defaults to 'datos-abiertos-agosto-2019.csv'.

    Returns:
        pandas.dataframe: dataframe with the meta data
    """
    logger = logging.getLogger('get_headers_from_metadata')

    # get full file path from the given bucket
    logger.info('Reading file: {}'.format(file))
    file = 'gs://{}/{}/data/{}'.format(bucket, directory, filename)
    df = pd.read_csv(file, encoding='latin1', sep=';')
    logger.info('Done!!')

    return df

def rename_colums(df, metadata):
    metadata_cols = metadata.columns()
    data_cols     = df.columns()

    cols_to_keep   = [col for col in data_cols if col in metadata_cols]
    cols_to_rename = [col for col in data_cols if col not in metadata_cols]
    
    return df
    # dict_cols_rename = dict()
    # for col in cols_to_rename:

def clean_strin_cols(df):
    return df

def save_data(df):
    pass

def run():
    df_metadata = get_headers_from_metadata()
    raw_data    = get_input_data()
    
    clean_data = rename_colums(df=raw_data, metadata=df_metadata)
    clean_data = clean_string_cols(df=clean_data)
    save_data(df=clean_data)

if __name__ == '__main__':
    # set up the logging output format (to visualize in a terminal)
    log_fmt = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    logging.captureWarnings(True)
    logging.basicConfig(
        level  = logging.INFO,
        format = log_fmt
    )
    run()

def clean_datetime_cols(
    df,
    initial_time_col = 'FECHA_INCIDENTE',
    final_time_col   = 'FECHA_INICIO_DESPLAZAMIENTO_MOVIL'
):
    
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
        new_date = parse(time) -relativedelta(minutes=np.median(list_minutes))
        list_new_dates.append(new_date)

    df_date_nan[initial_time_col] = list_new_dates
    df_out = pd.concat([df_date_nan, df_date_nonan])
    
    return df_out

