from camel_decorators import multiserver
from camel_utils_x.camel_sql import SQLBase
from dbx import Dbtools
import pandas as pd

import logging
from tqdm import tqdm
from ..extract.utils import server_open_nd


def payment_nd(server_ids = None, game = None, days = 30):
    game = game if game else 'aoz'
    tool = Dbtools.initialize('all',game)
    server_ids = server_ids if server_ids else sorted(list(tool.game_servers_parsed.keys()))
    server_open = server_open_nd(game, days = days, offset = 0)

    fields = ['currency_ammount']
    table = 'payment_record'
    parsers = { 'currency_ammount':'sum'}
    alias = ['amount']

    dfs = []
    pbar = tqdm(server_ids, desc = f'Paymen sum {days} day: ')

    for server_id in pbar:
        pbar.set_description('Paymen_nd {}'.format(server_id))

        try:
            connection = tool.get_connection(**tool.get_conn_info(server_id,'gs'))
        except:
            logging.error('Unable to connection server {}'.format(server_id))
            continue

        before_date = server_open.loc[server_id,f'open_{days}_date']
        sql = SQLBase(fields = fields, table = table,
              parsers = parsers,
              alias = alias).\
        stamp13beforeq('currency_ammount', before_date).make()

        result = pd.read_sql_query(sql, connection)
        result['from_server'] = server_id
        dfs.append(result)
    dfs=  pd.concat(dfs, sort = False)
    return dfs


def user_count_nd(server_ids = None, game = None, days = 30):
    game = game if game else 'aoz'
    tool = Dbtools.initialize('all',game)
    server_ids = server_ids if server_ids else sorted(list(tool.game_servers_parsed.keys()))
    server_open = server_open_nd(game, days = days, offset = 0)

    fields = ['distinct id']
    table = 'user_info'
    parsers = { 'distinct id':'count'}
    alias = ['register_count']

    dfs = []
    pbar = tqdm(server_ids, desc = f'User Count {days} day: ')

    for server_id in pbar:
        pbar.set_description('User Count {}'.format(server_id))

        try:
            connection = tool.get_connection(**tool.get_conn_info(server_id,'gs'))
        except:
            logging.error('Unable to connection server {}'.format(server_id))
            continue

        before_date = server_open.loc[server_id,f'open_{days}_date']
        sql = SQLBase(fields = fields, table = table,
              parsers = parsers,
              alias = alias).\
        stamp10beforeq('create_time', before_date).make()

        result = pd.read_sql_query(sql, connection)
        result['from_server'] = server_id
        dfs.append(result)
    dfs=  pd.concat(dfs, sort = False)
    return dfs


def payed_user_count_nd(server_ids = None, game = None, days = 30):
    game = game if game else 'aoz'
    tool = Dbtools.initialize('all',game)
    server_ids = server_ids if server_ids else sorted(list(tool.game_servers_parsed.keys()))
    server_open = server_open_nd(game, days = days, offset = 0)

    fields = ['distinct user_id']
    table = 'payment_record'
    parsers = { 'distinct user_id':'count'}
    alias = ['payed_user_count']

    dfs = []
    pbar = tqdm(server_ids, desc = f'Pay User Count {days} day: ')

    for server_id in pbar:
        pbar.set_description('Pay User Count {}'.format(server_id))

        try:
            connection = tool.get_connection(**tool.get_conn_info(server_id,'gs'))
        except:
            logging.error('Unable to connection server {}'.format(server_id))
            continue

        before_date = server_open.loc[server_id,f'open_{days}_date']
        sql = SQLBase(fields = fields, table = table,
              parsers = parsers,
              alias = alias).\
        stamp13beforeq('create_time', before_date).make()

        result = pd.read_sql_query(sql, connection)
        result['from_server'] = server_id
        dfs.append(result)
    dfs=  pd.concat(dfs, sort = False)
    return dfs


def device_count_nd(server_ids = None, game = None, days = 30):
    game = game if game else 'aoz'
    tool = Dbtools.initialize('all',game)
    server_ids = server_ids if server_ids else sorted(list(tool.game_servers_parsed.keys()))
    server_open = server_open_nd(game, days = days, offset = 0)

    fields = ['distinct uid']
    table = 'operate_device_log'
    parsers = { 'distinct uid':'count'}
    alias = ['device_count']

    dfs = []
    pbar = tqdm(server_ids, desc = f'Device Count {days} day: ')

    for server_id in pbar:
        pbar.set_description('Device Count {}'.format(server_id))

        try:
            connection = tool.get_connection(**tool.get_conn_info(server_id,'gs'))
        except:
            logging.error('Unable to connection server {}'.format(server_id))
            continue

        before_date = server_open.loc[server_id,f'open_{days}_date_id']
        sql = SQLBase(fields = fields, table = table,
              parsers = parsers,
              alias = alias).\
        lesseq('date_id', before_date).make()

        result = pd.read_sql_query(sql, connection)
        result['from_server'] = server_id
        dfs.append(result)
    dfs=  pd.concat(dfs, sort = False)
    return dfs




def payment_per_user_nd(server_ids, game, days):
    pay = paymen_nd(server_ids, game, days)
    users = user_count_nd(server_ids, game, days)
    user_pay = payment.merge(user, on = ['from_server'], how = 'inner')
    user_pay['amount_per_user'] = user_pay['amount'] / user_pay['register_count']
    return user_pay
