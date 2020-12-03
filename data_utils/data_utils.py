import pandas as pd
import numpy as np
from functools import wraps

def add_day_id(df, group_col = 'from_server', date_col = 'date', id_offset = 0, day_id_name = 'day_id'):
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



if __name__ == '__main__':
    @with_day_id()
    def get_df():
        from datetime import datetime
        from_server = np.random.choice([1,2,3], size = 100)
        dates = [datetime(2020,5,np.random.choice(np.arange(1,6))).date() for i in range(100)]
        df = pd.DataFrame(zip(from_server, dates), columns = ['from_server','date'])
        return df


    print(get_df().head(1000))
