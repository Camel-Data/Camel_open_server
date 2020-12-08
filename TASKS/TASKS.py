import numpy as np

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
    #  group by index          # columns         # calc val   # method  # missing fill
    (['from_server','day_id'],  [],              ['active_time'], [np.max],0),
    (['from_server','day_id'],  [],              ['active_time'], [np.mean],0),
    (['from_server','day_id'],  [],              ['active_time'], [np.std],0),
    (['from_server','day_id'],  [],              ['active_time'], [np.median],0),
)

# Aggregate to server, day level
PAYMENT_TASKS = (
    #  group by index          # columns         # calc val   # method  # missing fill
    (['from_server','day_id'],  [],              ["amount_sum"], [np.max, np.mean, np.std, np.median],0),
    (['from_server','day_id'],  [],              "*",            [np.count_nonzero, np.sum],0),

)

# Summarize payment features
# Amount spent for each server, each user on each day
# Amount spent for each server, each user on each day, each type
# Count purchase bills for each server, each user on each day
# Count purchase bills for each server, each user on each day, each type
PAYMENT_TO_DAILY = (
    #  group by index          # columns         # calc val   # method  # missing fill
    (['from_server','day_id','user_id'],[],                      ['amount'], [np.sum],0),
    (['from_server','day_id','user_id'],['type'],                ['amount'], [np.sum],0),
    (['from_server','day_id','user_id'],[],                      ['amount'], ['count'],0),
    (['from_server','day_id','user_id'],['type'],                ['amount'], [np.count_nonzero],0),
)


# If purchase type not appear on payment table, you should use this pipeline
PAYMENT_TO_DAILY_SIMPLE = (
    #  group by index          # columns         # calc val   # method  # missing fill
    (['from_server','day_id','user_id'],[],                      ['amount'], [np.sum],0),
    (['from_server','day_id','user_id'],[],                      ['amount'], ['count'],0),
)
