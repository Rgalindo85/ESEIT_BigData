# data_manipulator.py
import os
import logging

import pandas as pd

def get_input_data(bucket='esp-big-data', initial_directory='BigData', filename='datos-abiertos-agosto-2019.csv'):
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
    filepath = os.path.join(project_dir, 'data', 'raw')
    file     = os.path.join(filepath, filename)

    logger.info('Reading file: {}'.format(file))
    data = pd.read_csv(file, encoding='latin1', sep=';')
    logger.info('Done!!')
 
    return data


def get_headers_from_metadata(bucket='esp-big-data', directory='BigData', filename='metadatos-llamadas-urg-y-emer.csv'):
    """Read a csv file in a bucket of GCS, the file must use latin1 encoding and the separator is a semicolon (;)

    Args:
        bucket (str, optional): Name of the bucket. Defaults to 'esp-big-data'.
        directory (str, optional): project directory. Defaults to 'BigData'.
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

##Destino de archivos
def get_input_data_open(filename):
    current_dir = os.path.dirname(os.path.abspath(__file__)) # la ruta actual
    source_dir = current_dir[:current_dir.rfind('/')]
    project_dir = source_dir[:source_dir.rfind('/')]

    # filepath = project_dir = '/data/raw'
    filepath = os.path.join(project_dir, 'data', 'processed')
    file = os.path.join(filepath, filename)
    print('data path', file)
    opendata = pd.read_csv(file, encoding= 'latin1', sep=';', low_memory=False)

    return opendata



    

if __name__ == '__main__':
    get_input_data(None)
