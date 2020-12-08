import pytest
import config
from dbx import Dbtools
import numpy as np
from extract import open_server_users
from extract import open_server_battle
from extract import open_server_engage
from extract import open_server_battle
from extract import open_server_troop
from extract import open_server_resource
from extract import open_server_goldfarmer
from extract import open_server_payment

GAMES = ['aoz','wao']
N_SERVERS = 2

def test_user():

    for game in GAMES:
        fields = ["id","create_time","language","register_channel"]
        tool = Dbtools.initialize('all',game)
        server_ids = list(tool.game_servers_parsed.keys())
        server_ids = list(np.random.choice(server_ids,N_SERVERS))
        df = open_server_users(game = game,
            server_ids = server_ids ,fields = fields, days = 5, offset = 3)
        assert df is not None
        assert len(df) > 0


def test_payment():

    for game in GAMES:
        fields = ['user_id','create_time','currency_ammount', 'type']
        tool = Dbtools.initialize('all',game)
        server_ids = list(tool.game_servers_parsed.keys())
        server_ids = list(np.random.choice(server_ids,N_SERVERS))
        df = open_server_payment(game = game,
            server_ids = server_ids,fields = fields, days = 5, offset = 3)
        assert df is not None
        assert len(df) > 0
