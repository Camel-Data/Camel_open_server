from dbx import Dbtools
from camel_utils_x.camel_sql import SQLBase
from camel_queries.datafile_config import all_paths, show_catalog
from user_type_x import user_type_core
from ..data_utils import with_day_id
from .utils import rename_aggr, server_open_nd

from datetime import datetime, timedelta
import pandas as pd
import re

import logging
from tqdm import tqdm

@with_day_id()
def open_server_users(server_ids = [123,234], days = 10, server_open = None,
                    fields = None, game = 'aoz', offset = 0, **kwargs):

    '''
        This function queries user_info for all users registered within `days` days
        after server open.
        User type queries are included in this function.

        Parameters:
            server_ids: Integer of list of server_ids
            days: n days after server open
            server_open: dataframe or None,
                if pass a server_open dataframe, this data frame should contain 1 column
                `open_{n}_days`, where n == `days`, with dtype 'str'
            fields: data fields want from user_info table
            game: the game to query

        Returns:
            user info dataframe, without user type

        Note: you can generate open server dataframe like this:

        ```python
        from camel_queries.query_sso import query_server_open
        server_open = query_server_open()


        server_open[f'open_{days}_date'] = (server_open['open_time'] \
                                            + timedelta(days = 10)).dt.strftime('%Y-%m-%d')
        ```

    '''

    tool = Dbtools.initialize('all',game)

    # default parameters
    if isinstance(server_ids, int):
        server_ids = [server_ids]

    if server_open is None:
        server_open =server_open_nd(game = game,days = days, offset = offset)



    if not fields:
        fields = ['id','create_time','language','register_channel','login_channel',
                  'country_code', 'register_ip_country', 'bind_channel', 'device_id',
                  'device_model','source_channel']

    table = 'user_info'

    parsers = {'create_time':'stamp10'}

    dfs = []



    pbar = tqdm(server_ids, desc = 'UserInfo: ')

    for server_id in pbar:
        pbar.set_description('UserInfo: {}'.format(server_id))
        try:
            before_date = server_open.loc[server_id,f'open_{days}_date']
            after_date = server_open.loc[server_id, 'open_time']

            sql = SQLBase(fields = fields, table = table, parsers = parsers)\
                .stamp10before('create_time',before_date)\
                .stamp10aftereq('create_time', after_date)


            conn = tool.get_connection(**tool.get_conn_info(server_id,'gs'))
            df = pd.read_sql_query(sql.make(),conn)
            df['from_server'] = server_id
            dfs.append(df)
        except Exception as e:
            logging.error('Failed to get user info on server {}'.format(server_id))

    dfs = pd.concat(dfs, sort = False)

    dfs.columns = rename_aggr(df.columns, aggr = 'from_unixtime')
    dfs.rename(columns = {'id':'user_id'}, inplace = True)
    dfs['date'] = pd.to_datetime(dfs.create_time.dt.date)


    utypes = []
    pbar = tqdm(server_ids, desc = 'UserType: ')
    for server_id in pbar:
        pbar.set_description('UserType: {}'.format(server_id))
        try:
            logging.info('Determine user type on server {}'.format(server_id))
            utypes.append(user_type_core(game, server_id))


        except:
            logging.warning('Unable to determin user type on server {}'.format(server_id))


    try:
        utypes = pd.concat(utypes,sort = False)
        dfs = dfs.merge(utypes, on = ['user_id','from_server'], how = 'left')
        dfs['user_type'] = dfs.user_type.fillna('Missing')
    except:
        dfs['user_type'] = 'Missing'

    try:
        if 'device_model' in dfs.columns:
            dfs['device_model'] = ['IOS' if re.search(r".*iphone.*",str(dm), re.IGNORECASE) \
                                    else "ANDROID"  \
                                    for dm in dfs['device_model']]
    except:
        dfs['device_model'] = 'Missing'


    return dfs
