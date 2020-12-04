import numpy as np

USER_TASKS = (
    #  group by index          # columns         # calc val   # method  # missing fill
    (['from_server','day_id'],[],                ['user_id'], ['count'],0),
    (['from_server','day_id'],['language'],      ['user_id'], ['count'],0),
    (['from_server','day_id'],['source_channel'],['user_id'], ['count'],0),
    (['from_server','day_id'],['user_type'],     ['user_id'], ['count'],0),
    (['from_server','day_id'],['device_model'],  ['user_id'], ['count'],0),
    (['from_server','day_id'],['bind_channel'],  ['user_id'], ['count'],0),
)

ENGAGE_TASKS = (
    #  group by index          # columns         # calc val   # method  # missing fill
    (['from_server','day_id'],  [],              ['active_time'], [np.max],0),
    (['from_server','day_id'],  [],              ['active_time'], [np.mean],0),
    (['from_server','day_id'],  [],              ['active_time'], [np.std],0),
    (['from_server','day_id'],  [],              ['active_time'], [np.median],0),
)

PAYMENT_TASKS = (
    #  group by index          # columns         # calc val   # method  # missing fill
    (['from_server','day_id'],  [],              ['amount'], [np.max],0),
    (['from_server','day_id'],  [],              ['amount'], [np.mean],0),
    (['from_server','day_id'],  [],              ['amount'], [np.std],0),
    (['from_server','day_id'],  [],              ['amount'], [np.median],0),
)


