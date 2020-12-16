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



task = dict(game = 'aoz',server_ids = [234,235,236],
            offset = ETLConfig.OFFSET,days =ETLConfig.OPEN_SERVER_DAYS)

task['server_open'] = server_open_nd(**task)

server_features = feature_generation(task, ETLConfig)





from camel_open_server.targets import device_count_nd
from camel_open_server.targets import payed_user_count_nd
from camel_open_server.targets import user_count_nd

device_count_nd(server_ids = [100,101,102,103],game = 'aoz',days = 30)

user_count_nd(server_ids = [223,224,255])
