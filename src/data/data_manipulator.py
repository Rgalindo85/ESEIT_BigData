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


if __name__ == '__main__':
    # set up the logging output format (to visualize in a terminal)
    log_fmt = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    logging.captureWarnings(True)
    logging.basicConfig(
        level  = logging.INFO,
        format = log_fmt
    )

    df_metadata = get_headers_from_metadata()
    get_input_data()