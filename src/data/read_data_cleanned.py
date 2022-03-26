# #data_manipulator
import os
import pandas as pd


def get_input_data_open(filename):
    current_dir = os.path.dirname(os.path.abspath(__file__)) # la ruta actual
    source_dir = current_dir[:current_dir.rfind('/')]
    project_dir = source_dir[:source_dir.rfind('/')]

    # filepath = project_dir = '/data/raw'
    filepath = os.path.join(project_dir, 'data', 'processed')
    file = os.path.join(filepath, filename)
    print('data path', file)
    data = pd.read_csv(file, encoding= 'latin1', sep=';')

    return data



    

if __name__ == '__main__':
    get_input_data(None)
