import pandas as pd
import os
import logging

path = os.environ.get('PROJECT_PATH', '.') #C:\\Users\\anubi\\diploma for local developing
logging.info(f'working directory is {path}')
print(f'working directory is {path}')

def main():
    filename = f'{path}/data/diploma_main_dataset/ga_hits.csv'
    logging.info(f'start load file blablabla {filename}')
    print(f'filename is {filename}')
    with pd.read_csv(filename,chunksize=1000000) as reader:
        for chunk in reader:
            print(f'успешно загружено {list(chunk.index)[-1:]}')
    #print(df_hits)
    logging.info(f'successfully loaded {filename}')
    print(f'successfully loaded {filename}')


if __name__ == '__main__':
    main()

#import os

#path = os.environ.get('PROJECT_PATH','.')
#print(os.path.abspath(path))
#path_to_module = f'{path}/modules'

#print(path_to_module)
#print(os.listdir(path_to_module))