from camel_open_server.extract.utils import server_open_nd
from camel_open_server.extract import open_server_payment
from camel_open_server.extract import open_server_users
from camel_open_server.extract import open_server_engage
from camel_open_server.config import ETLConfig
from camel_open_server.targets import payment_nd

from camel_open_server.pipeline import feature_generation

server_open_nd('aoz', days = 10)
open_server_engage(server_ids = [1,2,35,6,7],days = 3, game = 'aoz')
open_server_users([1,2,3])
payment_nd(server_ids = list(range(300,400)),game = 'aoz', days = 30)


ETLConfig.OPEN_SERVER_DAYS


task = dict(game = 'aoz',server_ids = list(range(30,32)),
            offset = ETLConfig.OFFSET,days =ETLConfig.OPEN_SERVER_DAYS)

task['server_open'] = server_open_nd(**task)

server_features = feature_generation(task, ETLConfig)
from camel_open_server.extract import open_server_engage
open_server_engage(server_ids = list(range(30,31)),days = 10)



from camel_open_server.targets import device_count_nd
from camel_open_server.targets import payed_user_count_nd
from camel_open_server.targets import user_count_nd


import numpy as np
server_ids = np.random.choice(list(range(100,300)),size = 5, replace = False)
server_ids = list(server_ids)

device_count_nd(server_ids = server_ids,game = 'aoz',days = 30)


user_count_nd(server_ids = server_ids,game = 'aoz',days = 30)
from camel_open_server import server_open_nd
server_open_nd().loc[257]
