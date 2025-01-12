import pandas as pd
import psycopg2
import logging
import os
import numpy as np
import io, csv
from psycopg2.extensions import register_adapter, AsIs

path = os.environ.get('PROJECT_PATH', '.') #C:\\Users\\anubi\\diploma for local developing
logging.info(f'working directory is {path}')
print(f'working directory is {path}')
register_adapter(np.int64, AsIs)

class CSVFile(io.TextIOBase):
    # Create a CSV file from rows. Can only be read once.
    def __init__(self, rows, size=8192):
        self.row_iter = iter(rows)
        self.buf = io.StringIO()
        self.available = 0
        self.size = size

    def read(self, n):
        # Buffer new CSV rows until enough data is available
        buf = self.buf
        writer = csv.writer(buf)
        while self.available < n:
            try:
                row_length = writer.writerow(next(self.row_iter))
                self.available += row_length
                self.size = max(self.size, row_length)
            except StopIteration:
                break

        # Read requested amount of data from buffer
        write_pos = buf.tell()
        read_pos = write_pos - self.available
        buf.seek(read_pos)
        data = buf.read(n)
        self.available -= len(data)

        # Shrink buffer if it grew very large
        if read_pos > 2 * self.size:
            remaining = buf.read()
            buf.seek(0)
            buf.write(remaining)
            buf.truncate()
        else:
            buf.seek(write_pos)

        return data

def main_hits():
    filename = f'{path}/data/diploma_main_dataset/ga_hits.csv'
    logging.info(f'start load file {filename}')
    print(f'filename is {filename}')
    with pd.read_csv(filename, chunksize=1000000) as reader:
        for df_hits in reader:
            print(f'успешно загружено {list(df_hits.index)[-1:]}')
            logging.info(f'успешно загружено {list(df_hits.index)[-1:]}')
            df_hits.hit_date = pd.to_datetime(df_hits.hit_date, utc=True).dt.date
            df_hits_clean = df_hits.drop(['event_value'], axis=1)
            df_hits_clean = df_hits_clean.rename_axis('hit_id').reset_index()
            df_hits_clean.hit_time = df_hits_clean.hit_time.fillna(0)
            df_hits_clean.hit_time = df_hits_clean.hit_time.astype(int)
            df_hits_clean.event_label = df_hits_clean.event_label.fillna('other')
            df_hits_clean.hit_referer = df_hits_clean.hit_referer.fillna('other')
            #df_hits_clean['session_id'].to_csv('postgres-data/header.csv',index=False)#добавить колонки нужные
            data_list = [tuple(row) for row in df_hits_clean.itertuples(index=False)]
            print('data_list df_hits formed: ', len(data_list))
            logging.info(f'data_list df_hits formed length: {len(data_list)}')
            conn = None
            try:
                conn = psycopg2.connect('postgresql://data:data@postgres_data:5433/data')
                cur = conn.cursor()
                conn.autocommit = True
                print(f'всего к загрузке df_hits: {len(df_hits_clean)}')
                logging.info(f'всего к загрузке df_hits: {len(df_hits_clean)}')
                cur.copy_expert('COPY ga_hits (hit_id, session_id, hit_date, hit_time, hit_number, hit_type,'
                            'hit_referer, hit_page_path, event_category, event_action, event_label'
                            ') FROM STDIN WITH csv', CSVFile(data_list))
                cur.close()
            except(Exception, psycopg2.DatabaseError) as error:
                logging.info(error)
            finally:
                if conn is not None:
                    conn.close()
        logging.info(f'successfully loaded {filename}')

if __name__ == '__main__':
    main_hits()

# See PyCharm help at https://www.jetbrains.com/help/pycharm/
