from dbx import Dbtools
from camel_utils_x.camel_sql import SQLBase
from camel_queries.datafile_config import all_paths, show_catalog

from .utils import rename_aggr, server_open_nd

from datetime import datetime, timedelta
import pandas as pd

import logging
from tqdm import tqdm


def open_server_battle(server_ids = [123,234], days = 10, server_open = None,
                    fields = None, game = 'aoz', offset = 0):

    # default parameters
    if isinstance(server_ids, int):
        server_ids = [server_ids]

    if server_open is None:
        server_open =server_open_nd(game = game,days = days, offset = offset)

    if fields is None:
        fields = ['uid','date_id','pvp','fastpvp','gather_o + gather_r gather']

    # table
    table = 'operate_battle_log'

    # parsers
    parsers = {'date_id':'date'}

    dfs = []
    pbar = tqdm(server_ids, desc = 'Battle: ')
    for server_id in pbar:
        pbar.set_description('Battle {}'.format(server_id))
        try:
            tool = Dbtools.initialize('all',game)
            before_date = server_open.loc[server_id,f'open_{days}_date_id']
            after_date = server_open.loc[server_id,'open_date_id']

            sql = SQLBase(fields = fields, table = table,
                          parsers = parsers)\
                        .less('date_id',before_date)\
                        .moreq('date_id', after_date)\
                        .make()

            tool = Dbtools.initialize('all',game)
            conn = tool.get_connection(**tool.get_conn_info(server_id,'gs'))
            df = pd.read_sql_query(sql, conn)
            df['from_server'] = server_id
            dfs.append(df)
        except Exception as e:
            logging.error('Unable to get engagement on server {}'.format(server_id))
            print(e)

    dfs = pd.concat(dfs, sort = False)
    dfs.columns = rename_aggr(dfs.columns, aggr = 'date')
    dfs.rename(columns = {'uid':'user_id','date_id':'date'}, inplace = True)

    return dfs
