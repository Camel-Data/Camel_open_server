from camel_queries.query_sso import query_server_open
import re
from datetime import timedelta
import pandas as pd


def server_open_nd(game = 'aoz', days = 10, offset = 0, *arg, **kwargs):
    '''
        When query data that between server open data and n days after server open
        You need to know server open date
        And n day after server open date

        This function returns a dataframe containing 2 additional columns
        if days = 10:
            open_10_date: 'YYYY-MM-dd' to filter timestamp
            open_10_date_id: YYYYMMDD to filter date_id

    '''
    server_open = query_server_open(game)

    # server open time at 00:00:00 on open date
    server_open['open_time'] = pd.to_datetime(server_open.open_time.dt.date)\
     + timedelta(days = offset)

    # some table use yyyy-mm-dd to filter
    server_open[f'open_{days}_date'] = (server_open['open_time'] \
                                        + timedelta(days = days)).dt.strftime('%Y-%m-%d')

    # other tables user date_id as filter
    server_open[f'open_{days}_date_id'] = (server_open['open_time'] \
                                        + timedelta(days = days)).dt.strftime('%Y%m%d')

    # string date YYYY -MM -DD open date
    server_open['open_date'] = server_open.open_time.dt.strftime('%Y-%m-%d')

    # YYYYMMDD open dateid
    server_open['open_date_id'] = server_open.open_time.dt.strftime('%Y%m%d')

    server_open['open_date_id'] = server_open['open_date_id'].astype(int)
    server_open[f'open_{days}_date_id'] = server_open[f'open_{days}_date_id'].astype(int)

    return server_open


def rename_aggr(columns, aggr = 'from_unixtime'):
    cols = []
    for col in columns:
        find = re.search(aggr + r'\(([^)/]+)',col)
        if find:
            cols.append(find.groups()[0])
        else:
            cols.append(col)
    return cols
