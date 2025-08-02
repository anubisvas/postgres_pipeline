import psycopg2
from psycopg2.extras import execute_values
import logging
import os
import pandas as pd
import numpy as np
import glob
import json
from psycopg2.extensions import register_adapter, AsIs


path = os.environ.get('PROJECT_PATH', '.') #C:\\Users\\anubi\\diploma for local developing
register_adapter(np.int64, AsIs)
def insert_sessions():
    for item in glob.glob(f'{path}/data/diploma_extra_de_dataset/ga_sessions_new_????-??-??.json'):
        with (open(item, 'r') as fin):
            form = json.load(fin)
            forms = form[str(form.keys())[12:-3]]
            if not forms:
                continue
            data = pd.DataFrame.from_dict(forms)
            data_list = [tuple(row) for row in data.itertuples(index=False)]
            records_list_template = ','.join(['%s'])
            insert_query = ('insert into ga_sessions (session_id, client_id, visit_date, visit_time, visit_number,'
            'utm_source, utm_medium, utm_campaign, utm_adcontent, utm_keyword, device_category,'
            'device_os, device_brand, device_model, device_screen_resolution, device_browser,'
            'geo_country, geo_city) VALUES %s').format(records_list_template)
            conn = None
            try:
                conn = psycopg2.connect('postgresql://data:data@postgres_data:5433/data')
                cur = conn.cursor()
                conn.autocommit = True
                execute_values(cur,insert_query, data_list)
                logging.info(f'всего записей из json к загрузке в sessions: {len(forms)}')  #
                cur.close()
            except(Exception, psycopg2.DatabaseError) as error:
                logging.info(error)
            finally:
                if conn is not None:
                    conn.close()


if __name__ == '__main__':
    insert_sessions()

###    with conn.cursor(cursor_factory=NamedTupleCursor) as curs:
    ###     curs.execute('INSERT INTO '
    ###              'ga_sessions (session_id, client_id, visit_date, visit_time, visit_number, '
    ###              'utm_source, utm_medium, utm_campaign, utm_adcontent,'
    ###              'utm_keyword, device_category, device_os, device_brand,'
    ###              'device_model, device_screen_resolution, device_browser,'
    ######              'geo_country, geo_city) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s),'
###           ' (df_sessions_clean[0]))')
