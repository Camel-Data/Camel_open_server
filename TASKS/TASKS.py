import numpy as np

def nunique(a):
    return len(np.unique(a))

def greater_than_zero(a):
    return (a > 0).sum()


# Count user by each categorical feature
USER_TASKS = (
    #  group by index          # columns         # calc val   # method  # missing fill
    (['from_server','day_id'],[],                ['user_id'], ['count'],0),
    (['from_server','day_id'],['language'],      ['user_id'], ['count'],0),
    (['from_server','day_id'],['source_channel'],['user_id'], ['count'],0),
    (['from_server','day_id'],['user_type'],     ['user_id'], ['count'],0),
    (['from_server','day_id'],['device_model'],  ['user_id'], ['count'],0),
    (['from_server','day_id'],['bind_channel'],  ['user_id'], ['count'],0),
)

# Aggregate to server, day level
ENGAGE_TASKS = (
    (['from_server','day_id'],  [], ['active_time'], [np.max, np.mean, np.std, np.median, greater_than_zero],0),
)

# Aggregate to server, day level
PAYMENT_TASKS = (
    (['from_server','day_id'],  [], ["amount_sum"], [np.max, np.mean, np.std, np.median],0),
    (['from_server','day_id'],  [],  "*", [np.count_nonzero, np.sum],0),

)

# Summarize payment features
# Amount spent for each server, each user on each day
# Amount spent for each server, each user on each day, each type
# Count purchase bills for each server, each user on each day
# Count purchase bills for each server, each user on each day, each type
PAYMENT_TO_DAILY = (
    (['from_server','day_id','user_id'],[], ['amount'], [np.sum],0),
    (['from_server','day_id','user_id'],['type'], ['amount'], [np.sum],0),
    (['from_server','day_id','user_id'],[],['amount'], ['count'],0),
    (['from_server','day_id','user_id'],['type'],['amount'], [np.count_nonzero],0),
)

# If purchase type not appear on payment table, you should use this pipeline
PAYMENT_TO_DAILY_SIMPLE = (
    (['from_server','day_id','user_id'],[], ['amount'], [np.sum],0),
    (['from_server','day_id','user_id'],[], ['amount'], ['count'],0),
)

BATTLE_TASK = (
    (['from_server','day_id'],[], ['user_id'], ['count'],0),
    (['from_server','day_id'],[], "*", ['sum'],0),
)

troop_nums = ['losePower', 'raisePower', 'loseTroop', 'woundTroop',
       'addTroop', 'dead1', 'dead2', 'dead3', 'dead4', 'dead5', 'dead6',
       'dead7', 'dead8', 'dead9', 'dead10', 'dead11', 'wound1', 'wound2',
       'wound3', 'wound4', 'wound5', 'wound6', 'wound7', 'wound8', 'wound9',
       'wound10', 'wound11']

TROOP_TASK = (
    (['from_server','day_id'],[], troop_nums, [np.sum],0),
    (['from_server','day_id'],[], ['alliance_id'], [np.count_nonzero],0),
)

resource_states = [ 'mojo','food','oil','rare_earth','iron','high_energe_ore']
resource_changes = ['mojo_add', 'mojo_use','food_add', 'food_use','oil_add', 'oil_use',
                    'rare_earth_add', 'rare_earth_use','iron_add', 'iron_use','high_energe_ore_add',
                    'high_energe_ore_use']

lvls = ['city_level']

RESOURCE_TASK = (
    (['from_server','day_id'],[],resource_changes, ['sum'],0),
)


CITYLEVEL_TASK = task = (
    (['from_server','day_id'],['city_level'],['user_id'],['count'],0),
)

available = {
    'USER_TASKS':USER_TASKS,
    'ENGAGE_TASKS':ENGAGE_TASKS,
    'PAYMENT_TASKS':PAYMENT_TASKS,
    'PAYMENT_TO_DAILY':PAYMENT_TO_DAILY,
    'PAYMENT_TO_DAILY_SIMPLE':PAYMENT_TO_DAILY_SIMPLE,
    'BATTLE_TASK':BATTLE_TASK,
    'TROOP_TASK':TROOP_TASK,
    'RESOURCE_TASK':RESOURCE_TASK
}
