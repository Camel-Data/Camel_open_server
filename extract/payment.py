from dbx import Dbtools
from camel_utils_x.camel_sql import SQLBase
from camel_queries.datafile_config import all_paths, show_catalog

from .utils import rename_aggr, server_open_nd

from datetime import datetime, timedelta
import pandas as pd

import logging
from tqdm import tqdm


def open_server_payment(server_ids = [123,234], days = 10, server_open = None,
                    fields = None, game = 'aoz', offset = 0, **kwargs):

    # default parameters
    if isinstance(server_ids, int):
        server_ids = [server_ids]

    if server_open is None:
        server_open =server_open_nd(game = game,days = days, offset = offset)

    if fields is None:
        fields = ['user_id','create_time','currency_ammount']

    # table
    table = 'payment_record'

    # parsers
    parsers = {'create_time':'stamp13'}

    alias = {'currency_ammount':'amount'}

    dfs = []
    pbar = tqdm(server_ids, desc = 'Payment: ')
    for server_id in pbar:
        pbar.set_description('Payment {}'.format(server_id))
        try:
            tool = Dbtools.initialize('all',game)
            before_date = server_open.loc[server_id,f'open_{days}_date']
            after_date = server_open.loc[server_id, 'open_time']

            sql = SQLBase(fields = fields, table = table,
                          parsers = parsers, alias = alias)\
                        .stamp13before('create_time',before_date)\
                        .stamp13after('create_time',after_date).make()

            tool = Dbtools.initialize('all',game)
            conn = tool.get_connection(**tool.get_conn_info(server_id,'gs'))
            df = pd.read_sql_query(sql, conn)
            df['from_server'] = server_id
            dfs.append(df)
        except Exception as e:
            logging.error('Unable to get payment on server {}'.format(server_id))
            print(e)

    dfs = pd.concat(dfs, sort = False)
    dfs.columns = rename_aggr(dfs.columns, aggr = 'from_unixtime')
    dfs.rename(columns = {'create_time':'date'}, inplace = True)

    return dfs
