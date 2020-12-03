
from functools import reduce
import pandas as pd
from config import config_list

def gold_farmer_aggr(goldfarmer, etl_config = config_list['ETLConfig']):
    '''
        Aggregate goldfarmer table

        Note: you need to fill missing values on returned dataframe.

        Parameters:
        --------------
        goldfamer: dataframe contain data in gold_farmer_rob table,
        you must include `atk_id`, 'def_id','date' in this dataframe

        etl_config: a class of configurations,

        ```python
            class SomeCconfig:
                GOLD_FARMER_ATTK = [....] # must include `atk_id`,'date'
                ATTK_GROUPBY = [....] # always atk_id, date
                ATTK_LAST = [....] # can be empty list
                ATTK_SUM = [....] # can be empty list
                ATTK_COUNT = [....] # can be empty list
                GOLD_FARMER_DEF = [....] # must include `def_id`,'date'
                DEF_GROUPBY = [....] # always def_id, date
                DEF_LAST = [....] # can be empty list
                DEF_SUM = [....] # can be empty list
                DEF_COUNT = [....]     # can be empty list
        ```
        here, empty list is just empty list,  not None, or missing.

        Returns:
        --------------
        An aggregated dataframe, each user each day, one record.

    '''

    # retrieve attaker fields, aggregate
    attk_df = goldfarmer[etl_config.GOLD_FARMER_ATTK + ['date']].groupby(etl_config.ATTK_GROUPBY)
    attk_lasts = attk_df[etl_config.ATTK_LAST].last()
    attk_sums = attk_df[etl_config.ATTK_SUM].sum()
    attk_counts = attk_df[etl_config.ATTK_COUNT].count()

    # retrieve defender fields, aggregate
    def_df = goldfarmer[etl_config.GOLD_FARMER_DEF + ['date']].groupby(etl_config.DEF_GROUPBY)
    def_lasts = def_df[etl_config.DEF_LAST].last()
    def_sums = def_df[etl_config.DEF_SUM].sum()
    def_counts = def_df[etl_config.DEF_COUNT].count()

    # merge attck and defence
    atk_agg = reduce(lambda x,y:x.merge(y, left_index = True, right_index = True, how = 'outer'),
        [attk_lasts,attk_sums, attk_counts]).reset_index()

    def_agg = reduce(lambda x,y:x.merge(y, left_index = True, right_index = True, how = 'outer'),
        [def_lasts, def_sums, def_counts]).reset_index()

    # rename and merge on attack and defence
    atk_rename_dict = {'atk_id':'user_id','id':'atk_count'} \
        if 'id' in etl_config.ATTK_COUNT else {'atk_id':'user_id'}

    def_rename_dict = {'def_id':'user_id','id':'def_count'} \
        if 'id' in etl_config.DEF_COUNT else {'def_id':'user_id'}

    atk_agg = atk_agg.rename(columns = atk_rename_dict)
    def_agg = def_agg.rename(columns = def_rename_dict)

    gold_farmer_agg = atk_agg.merge(def_agg, on = ['user_id','date'], how = 'outer')

    # imputer - fill 0
    gold_farmer_agg[etl_config.ATTK_SUM] = gold_farmer_agg[etl_config.ATTK_SUM].fillna(0)
    gold_farmer_agg[etl_config.DEF_SUM] = gold_farmer_agg[etl_config.DEF_SUM].fillna(0)

    # imputer - state features
    attk_lasts = gold_farmer_agg[['user_id','date'] + etl_config.ATTK_LAST]
    def_lasts = gold_farmer_agg[['user_id','date'] + etl_config.DEF_LAST]

    attk_lasts.columns = [col if not col.startswith('atk') else col.replace('atk_','')
                      for col in attk_lasts.columns]

    def_lasts.columns = [col if not col.startswith('def') else col.replace('def_','')
                      for col in def_lasts.columns]

    user_daily = attk_lasts.combine_first(def_lasts)

    count_cols = [col for col in gold_farmer_agg.columns if col.endswith('_count')]
    user_daily = user_daily.merge(gold_farmer_agg[etl_config.ATTK_SUM + etl_config.DEF_SUM + count_cols + ['user_id','date']],how = 'left',on = ['user_id','date'])
    user_daily = user_daily.fillna(0)


    return user_daily
