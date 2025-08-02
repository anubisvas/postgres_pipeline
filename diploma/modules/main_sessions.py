import numpy as np
import pandas as pd
import os
import io, csv
import psycopg2
import logging
from psycopg2.extensions import register_adapter, AsIs

path = os.environ.get('PROJECT_PATH','.') #C:\\Users\\anubi\\diploma for local developing
logging.info(f'working directory is {path}')
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

def main_sessions():
    filename = f'{path}/data/diploma_main_dataset/ga_sessions.csv'
    df_sessions = pd.read_csv(filename, low_memory=False)
    logging.info(f'filename is {filename}')
    df_sessions.visit_date = pd.to_datetime(df_sessions.visit_date, utc=True).dt.date
    df_sessions_clean=df_sessions.copy()
    df_sessions_clean[[
        'utm_source','utm_campaign','utm_adcontent','device_brand','device_os','utm_keyword','device_model'
    ]] = df_sessions[[
        'utm_source', 'utm_campaign', 'utm_adcontent', 'device_brand', 'device_os', 'utm_keyword', 'device_model'
    ]].fillna('other')
    data_list = [tuple(row) for row in df_sessions_clean.itertuples(index=False)]
    logging.info(f'data_list df_sessions formed: {len(data_list)}')

    conn = None
    try:
        conn = psycopg2.connect('postgresql://data:data@postgres_data:5433/data')
        cur = conn.cursor()
        conn.autocommit = True
        cur.copy_expert('COPY ga_sessions (session_id, client_id, visit_date, visit_time, visit_number,'
                    'utm_source, utm_medium, utm_campaign, utm_adcontent, utm_keyword, device_category,'
                    'device_os, device_brand, device_model, device_screen_resolution, device_browser,'
                    'geo_country, geo_city) FROM STDIN WITH csv', CSVFile(data_list))
        logging.info(f'всего к загрузке df_sessions: {len(data_list)}')  #
        cur.close()
    except(Exception, psycopg2.DatabaseError) as error:
        logging.info(error)
    finally:
        if conn is not None:
            conn.close()


if __name__ == '__main__':
    main_sessions()