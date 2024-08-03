import os
import connection
import sqlparse
import pandas as pd

if __name__ == '__main__':
    #connection for data source
    conf = connection.config('marketplace_prod')
    conn, engine = connection.get_conn(conf, 'DataSource')
    cursor = conn.cursor()


    # connection dwh
    conf_dwh = connection.config('dwh')
    conn_dwh, engine_dwh = connection.get_conn(conf_dwh, 'DWH')
    cursor_dwh = conn_dwh.cursor()


    #get query string
    path_query = os.getcwd() + '/query/'
    query = sqlparse.format(
        open(path_query + 'query.sql','r').read(), strio_comments = True
    ).strip()
    dwh_design = sqlparse.format(
        open(path_query + 'dwh_design.sql','r').read(), strio_comments = True
    ).strip()
    

try:
    #get data
    print('[INFO] service etl is running..')
    df = pd.read_sql(query, engine)

    # create schema dwh
    cursor_dwh.execute(dwh_design)
    conn_dwh.commit()

    #ingrest data to dwh
    df.to_sql(
        'dim_orders_psq',
        engine_dwh,
        schema='public',
        if_exists='append', #replace or append
        index=False
    )
    print('[INFO] etl success')
except Exception as e:
    print('[INFO] service etls is failed')
    print(str(e))