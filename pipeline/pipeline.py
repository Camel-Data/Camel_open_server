from extract import open_server_users
from extract import open_server_engage
from extract import open_server_payment
from extract import open_server_battle
from extract import open_server_resource
from extract import open_server_troop
from extract import server_open_nd
from extract import open_server_goldfarmer
from transform import goldfarmer_transform
from config import ETLConfig
import TASKS as TASKS
from data_utils import run_aggregation_tasks, asfreq_by_group
import numpy as np
import pandas as pd
from functools import reduce
from datetime import datetime, timedelta

# this is just a default parameter
task_config = dict(game = 'aoz',server_ids = [234,235,236],
            offset = ETLConfig.OFFSET,days =ETLConfig.OPEN_SERVER_DAYS)



def gen_city_level_features(resource):
    '''

        DESC:
        -------------------------------
        Generate city_level features from resource table
        For each day: count number of users at city_leve=1, citylevel=2....

        Parameters:
        -------------------------------
        resource: Dataframe of operate_res_log, contain day_id, date, user_id , from_server fields

        Returns:
        -------------------------------
        DataFrame of features by [server, day] level
    '''
    resource['city_level'] = resource['city_level'].astype(int)
    level_features_all_server = []

    # Loop through each server
    for server_id in resource.from_server.unique():

        # select current server data
        sub_resource = resource[resource.from_server == server_id].copy()
        sub_resource['date'] = pd.to_datetime(sub_resource['date'])
        sub_resource = sub_resource[['user_id','date','day_id','city_level']]

        # keep a record of date mapping
        mapping = sub_resource[['date','day_id']].drop_duplicates().values
        mapping = dict(zip(mapping[:,0], mapping[:,1]))

        # as freq operation
        level_features = asfreq_by_group(sub_resource)
        level_features['day_id'] = level_features['date'].replace(mapping)
        level_features['from_server'] = server_id
        level_features_all_server.append(level_features)
    city_level = pd.concat(level_features_all_server, sort = False)
    city_level['city_level'] = city_level['city_level'].round().astype(int)

    return run_aggregation_tasks(city_level, TASKS.CITYLEVEL_TASK, merge = True)


def feature_generation(task_config = task_config, etl_config = ETLConfig):

    # query server open date
    task_config['server_open'] = server_open_nd(**task_config)

    # query user info
    user = open_server_users(**task_config, fields = etl_config.USER_INFO_FIELDS)

    # query engage
    engage = open_server_engage(**task_config, fields = etl_config.ENGAGE_FIELDS)

    # query engage, convert to each user 1 record pey day
    payment = open_server_payment(**task_config, fields = etl_config.PAYMENT_FIELDS)
    payment =run_aggregation_tasks(payment, TASKS.PAYMENT_TO_DAILY,merge= True).reset_index()

    # query battle
    battle = open_server_battle(**task_config, fields = etl_config.BATTLE_FIELDS)

    # query resource
    resource = open_server_resource(**task_config, fields = etl_config.RESOURCE_FIELDS)

    # query troop
    troop = open_server_troop(**task_config,fields = etl_config.TROOP_FIELDS)

    # query goldfarmer, convert to each user 1 record per day
    goldfarmer = open_server_goldfarmer(**task_config, fields = etl_config.GOLD_FARMER_FIELDS)
    goldfarmer = goldfarmer_transform(goldfarmer, etl_config)

    # aggregate to serve - day level
    user_features = run_aggregation_tasks(user, TASKS.USER_TASKS,merge = True)
    user_features = user_features.rename(columns = {'user_id_count':'register_count'})

    engage_features = run_aggregation_tasks(engage, TASKS.ENGAGE_TASKS, merge = True)

    battle_features = run_aggregation_tasks(battle, TASKS.BATTLE_TASK, merge = True)
    battle_features = battle_features.rename(columns = {'user_id_count':'battle_count'})

    troop_features = run_aggregation_tasks(troop, TASKS.TROOP_TASK, merge = True)

    resource_features = run_aggregation_tasks(resource, TASKS.RESOURCE_TASK,merge = True)

    city_level_features = gen_city_level_features(resource)

    feature_list = [user_features, engage_features, battle_features, troop_features, resource_features, city_level_features]

    return reduce(lambda left, right: left.merge(right, left_index = True, right_index = True), feature_list)
