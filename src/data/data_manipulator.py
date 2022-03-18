# data_manipulator.py
import os
import logging

import pandas as pd

def get_input_data(bucket='esp-big-data', initial_directory='BigData', filename='datos-abiertos-agosto-2019.csv'):

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

if __name__ == '__main__':
    # set up the logging output format (to visualize in a terminal)
    log_fmt = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    logging.captureWarnings(True)
    logging.basicConfig(
        level=logging.INFO,
        format=log_fmt
    )

    get_input_data()