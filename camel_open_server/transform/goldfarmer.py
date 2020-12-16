from functools import reduce
import pandas as pd
from ..config import config_list
from ..data_utils import with_day_id

def attacker_user_daily(goldfarmer, etl_config = config_list['ETLConfig']):
    '''Daily user behavior as attacker'''
    columns = list(set(etl_config.GOLD_FARMER_ATTK).union(etl_config.ATTK_GROUPBY))
    attk_df = goldfarmer[columns].groupby(etl_config.ATTK_GROUPBY)
    attk_lasts = attk_df[etl_config.ATTK_LAST].last()
    attk_sums = attk_df[etl_config.ATTK_SUM].sum()
    attk_counts = attk_df[etl_config.ATTK_COUNT].count()
    atk_agg = reduce(lambda x,y:x.merge(y, left_index = True, right_index = True, how = 'outer'),
        [attk_lasts,attk_sums, attk_counts]).reset_index()
    atk_rename_dict = {'atk_id':'user_id','id':'atk_count'} \
        if 'id' in etl_config.ATTK_COUNT else {'atk_id':'user_id'}
    atk_agg = atk_agg.rename(columns = atk_rename_dict)
    return atk_agg

def defender_user_daily(goldfarmer, etl_config = config_list['ETLConfig']):
    '''Daily user behavior as defender'''
    columns = list(set(etl_config.GOLD_FARMER_DEF).union(etl_config.DEF_GROUPBY))
    def_df = goldfarmer[columns].groupby(etl_config.DEF_GROUPBY)
    def_lasts = def_df[etl_config.DEF_LAST].last()
    def_sums = def_df[etl_config.DEF_SUM].sum()
    def_counts = def_df[etl_config.DEF_COUNT].count()
    def_agg = reduce(lambda x,y:x.merge(y, left_index = True, right_index = True, how = 'outer'),
        [def_lasts, def_sums, def_counts]).reset_index()
    def_rename_dict = {'def_id':'user_id','id':'def_count'} \
        if 'id' in etl_config.DEF_COUNT else {'def_id':'user_id'}
    def_agg = def_agg.rename(columns = def_rename_dict)
    return def_agg


def goldfarmer_user_daily(atk_agg, def_agg, etl_config = config_list['ETLConfig']):
    '''Either defender or attacker'''
    index_columns = ['user_id','date','from_server']
    gold_farmer_agg = atk_agg.merge(def_agg, on = index_columns, how = 'outer')

    # Handle sum features
    gold_farmer_agg[etl_config.ATTK_SUM] = gold_farmer_agg[etl_config.ATTK_SUM].fillna(0)
    gold_farmer_agg[etl_config.DEF_SUM] = gold_farmer_agg[etl_config.DEF_SUM].fillna(0)
    sum_features = gold_farmer_agg[index_columns + etl_config.ATTK_SUM + etl_config.DEF_SUM]

    # Handle state features
    attk_lasts = gold_farmer_agg[index_columns + etl_config.ATTK_LAST]
    def_lasts = gold_farmer_agg[index_columns + etl_config.DEF_LAST]
    attk_lasts.columns = [col if not col.startswith('atk') else col.replace('atk_','')
                      for col in attk_lasts.columns]
    def_lasts.columns = [col if not col.startswith('def') else col.replace('def_','')
                      for col in def_lasts.columns]
    state_features = attk_lasts.combine_first(def_lasts)


    # Handle count features
    count_cols = [col for col in gold_farmer_agg.columns if col.endswith('_count')]
    count_features = gold_farmer_agg[index_columns + count_cols]

    all_df = [sum_features, state_features, count_features]
    return reduce(lambda left, right: left.merge(right, on = index_columns, how = 'left'),all_df).fillna(0)

@with_day_id()
def goldfarmer_transform(goldfarmer, etl_config = config_list['ETLConfig']):
    atk_agg = attacker_user_daily(goldfarmer, etl_config)
    def_agg = defender_user_daily(goldfarmer, etl_config)
    return goldfarmer_user_daily(atk_agg, def_agg, etl_config)
