import pandas as pd
import numpy as np
from functools import wraps
from functools import reduce
import inspect
import logging
from collections.abc import Container, Iterable, Sequence
from dbx import Dbtools
from datetime import timedelta


def add_day_id(df, group_col = 'from_server', date_col = 'date',
                id_offset = 0, day_id_name = 'day_id'):
    '''Add a day_id to each data referenced to server open date'''
    result = []
    unique_servers = sorted(df[group_col].unique())

    for server_id in unique_servers:
        unique_dates = sorted(df[df.from_server == server_id ][date_col].unique())
        result += [[date, i + id_offset + 1, server_id ] for i, date in enumerate(unique_dates)]

    result = pd.DataFrame(result, columns = [date_col,day_id_name,group_col])
    return df.merge(result, on = [date_col, group_col], how = 'inner')


def with_day_id(*oargs, **okwargs):
    '''
        Allow parameters:
        group_col: like server_id, from_server
        date_col: YYYY-MM-DD like
        id_offset: day_id startswith
        day_id_name: rename day_id column
    '''
    def decorator(f):
        @wraps(f)
        def wrapper(*args, **kwargs):
            df = f(*args, **kwargs)
            return add_day_id(df,*oargs,**okwargs)
        return wrapper
    return decorator



def piv(df, task):
    '''Running a pivot task'''
    ind, cols, vals, methods, na = task
    piv = df.pivot_table(index = ind,
                 columns = cols,
                 values = vals,
                 aggfunc=methods).fillna(na)

    if piv.columns.names[-1]:
        names = []
        for op_type, agg_field, by in piv.columns:
            name = f'{piv.columns.names[-1]}_{by}_{op_type}_{agg_field}'
            names.append(name)
        piv.columns = names

    else:
        piv.columns = ['_'.join(piv.columns[0])]
    return piv


def groupby(df, task):
    '''Running a groupby task'''
    ind, cols, vals, methods, na = task
    if vals == '*':
        vals = [s for s in df.columns if s not in ['from_server','day_id','user_id','date']]

    df = df.groupby(ind)[vals].agg(methods)
    df.columns = [f'{field}_{method}'for field, method in df.columns]
    return df


def run_aggregation_tasks(df, tasks, merge = False):
    """
        Desc:
        --------------
        Function that run pivot/groupby tasks
        When you run df.pivot_table(index = .., columns = ..., values = ..., aggfunc = ..., ).fillna(...)
        Or you run df.groupby(by = ...)[...columns].agg(...)
        Parameters are (index, columns, values, aggfunc, na),
        So you define tuple of tuples of parameters, like,

        Each sub-tuple is a group tasks with parameter properly defined
        ```python
        USER_TASKS = (
            #  group by index          # columns         # calc val   # method  # missing fill
            (['from_server','day_id'],[],                ['user_id'], ['count'],0),
            (['from_server','day_id'],['language'],      ['user_id'], ['count'],0),
            (['from_server','day_id'],['source_channel'],['user_id'], ['count'],0),
            (['from_server','day_id'],['user_type'],     ['user_id'], ['count'],0),
            (['from_server','day_id'],['device_model'],  ['user_id'], ['count'],0),
            (['from_server','day_id'],['bind_channel'],  ['user_id'], ['count'],0),
        )
        ```

        Writing the task definition can be quiet boring, you can import TASKS already exists

        ```python
        from TASKS import USER_TASKS
        ```

        AND RUN
        ```python
        run_groupby_tasks(df, USER_TASKS, merge = True)
        ```
        to get the dataframe returned.

    """

    dfs = []
    for task in tasks:

        if len(task[1]) >0:
            dfs.append(piv(df,task))
        else:
            dfs.append(groupby(df,task))
    if merge:
        return reduce(lambda x,y:x.merge(y, left_index = True, right_index = True, how = 'left'), dfs)
    return dfs


def asfreq_by_group(df, group_col = 'user_id', date_col = 'date', fixed_last_day = None):

    # get minimm date of a user, and maximum
    first = df.groupby('user_id')['date'].min().reset_index()

    if not fixed_last_day:
        last = df.groupby('user_id')['date'].max().reset_index()

    # if you want to end at specific day
    else:
        last = first.copy()

        if isinstance(fixed_last_day, str):
            try:
                fixed_last_day = datetime.strptime(fixed_last_day,'%Y-%m-%d')
            except Exception as e:
                print('Bad datetime format for `fixed_last_day`')
                raise ValueError('Bad datetime format for `fixed_last_day`')

        last['date'] = fixed_last_day


    first = first.rename(columns = {'date':'first_date'})
    last = last.rename(columns = {'date':'last_date'})

    user_dates = first.merge(last, on = ['user_id'], how = 'left')


    user_dates = user_dates.loc[user_dates.first_date <= user_dates.last_date,:]


    # why use a loop: get faster speed
    results = []
    for user, s, e in user_dates.values:
        cur_date = s
        while s<= e:
            results.append([user,s])
            s += timedelta(days = 1)
    filled = pd.DataFrame(results, columns = ['user_id','date'])

    filled = filled.merge(df, on = ['user_id','date'], how = 'left').ffill()

    return filled




if __name__ == '__main__':
    @with_day_id()
    def get_df():
        from datetime import datetime
        from_server = np.random.choice([1,2,3], size = 100)
        dates = [datetime(2020,5,np.random.choice(np.arange(1,6))).date() for i in range(100)]
        df = pd.DataFrame(zip(from_server, dates), columns = ['from_server','date'])
        return df


    print(get_df().head(1000))
