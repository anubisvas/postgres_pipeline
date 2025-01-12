import psycopg2
import pandas as pd
import glob
import os
import json
from psycopg2 import sql
import logging
import numpy as np
from psycopg2.extensions import register_adapter, AsIs
from psycopg2.extras import execute_values

path = os.environ.get('PROJECT_PATH', '.') #C:\\Users\\anubi\\diploma for local developing
logging.info(f'working directory is {path}')
register_adapter(np.int64, AsIs)
def ask_new_id():
    stmt = sql.SQL('SELECT hit_id FROM ga_hits WHERE hit_id = (SELECT MAX(hit_id) FROM ga_hits)')
    conn1 = None
    try:
        conn1 = psycopg2.connect('postgresql://data:data@postgres_data:5433/data')
        logging.info(f'my connection {conn1}')
        with conn1.cursor() as cur:
            conn1.autocommit = True
            cur.execute(stmt)
            for row in cur:
                new_hit_id = row[0]
                logging.info(new_hit_id)
            cur.close()
    except(Exception, psycopg2.DatabaseError) as error:
        logging.info(error)
    finally:
        if conn1 is not None:
            conn1.close()
    return new_hit_id

def insert_hits():
    for item in glob.glob(f'{path}/data/diploma_extra_de_dataset/ga_hits_new_????-??-??.json'):
        logging.info(f'json filename is {item}') #item - name of json file
        new_hit_id = ask_new_id()
        """Add data to the table."""
        with (open(item, 'r') as fin):
            form = json.load(fin)
            forms = form[str(form.keys())[12:-3]]
            if not forms:
                continue
            new_id = int(new_hit_id) # надо будет заменить на автоматический фетч из бд
            idx=0 # 15726470 число записей в оригинальном датасете, который уже в БД
            data = pd.DataFrame.from_dict(forms)
            data.insert(loc=idx, column='hit_id',value=[x for x in range(new_id+1,new_id+(len(data)+1))])
            data = data.drop('event_value',axis=1)
            data_list = [tuple(row) for row in data.itertuples(index=False)]
            records_list_template = ','.join(['%s'])
            insert_query = ('insert into ga_hits (hit_id, session_id, hit_date, hit_time, hit_number, hit_type,'
                            'hit_referer, hit_page_path, event_category, event_action, event_label) VALUES %s'
                            ).format(records_list_template)
            conn = None
            try:
                conn = psycopg2.connect('postgresql://data:data@postgres_data:5433/data')
                cur = conn.cursor()
                conn.autocommit = True
                execute_values(cur,insert_query, data_list)
                logging.info(f'всего к загрузке {len(forms)}, осталось загрузить {(len(forms) - (new_id - new_hit_id))}')
                cur.close()
            except(Exception, psycopg2.DatabaseError) as error:
                logging.info(error)
            finally:
                if conn is not None:
                    conn.close()


if __name__ == '__main__':
    insert_hits()