'''
Module for wrangling data.

You'll want to use two main functions:
- call run() to return a clean data frame with net entry and exit data
calculated. You can include positional arguments for run in the form YYMMDD to
specify which weeks are pulled. Default is the week ending in 06/27. If one
argument given, will create dataframe for week specified. If two arguments
given, will create data frame for all weeks between the first argument and the
second argument.
- call agg_by(df, args) to return a data frame with entry and exit data summed
according to args. Possible args are currently 'date', 'time', 'booth',
'station', or some combination of 'date' or 'time' and 'booth' or 'station'.

Changes as of 07/01:
- added optional args to run() to specify week(s) of data to load as df
- added 'booth' and 'time' options to agg_by() + cleaned it up
- added time column to make it easier to use a variety of options in agg_by

7/01 Changes 2.0
- jk about the time column. In fact, removed date column too
- added 'day' and 'week/day' options to agg_by to enable sorting by day of week
    or by whether a day is a weekday or a weekend day

'''

import numpy as np
import pandas as pd
from datetime import date, datetime, timedelta

# Deals with SettingWithCopyWarning
pd.options.mode.chained_assignment = None

# Column rename mapping
COLUMNS =  {'DATE_TIME': 'datetime',
            'C/A':       'c_a',
            'UNIT':      'unit',
            'SCP':       'scp',
            'STATION':   'station',
            'LINENAME':  'linename',
            'DIVISION':  'division',
            'DESC':      'desc',
            'ENTRIES':   'entries',
            'EXITS':     'exits'}


def read_file(dt, data_dir='./mta_data/'):
    '''
    Assumes data files are in ./mta_data/ directory
    Args:
        dt (str): yyyy-mm-dd format date
    '''
    assert isinstance(dt, str), 'Date must be in yymmdd or yyyy-mm-dd format.'
    assert len(dt) in [6,10]
    dname = dt if len(dt) == 6 else dt[2:4]+dt[5:7]+dt[8:10]
    df = pd.DataFrame()
    try:
        df = pd.read_csv(data_dir+'turnstile_{}.txt'.format(dname), 
                                            parse_dates=[['DATE', 'TIME']])
        df.columns = list(map((lambda x: x.strip() if isinstance(x, str) else x), 
                      df.columns.values))

        df = df.rename(columns=COLUMNS)
    except:
        pass  # file does not exist
    return df

def read_files(dts, data_dir='./mta_data/'):
    '''
    Reads multiple files and returns one single DataFrame

    Args:
        dts (list): list of dates in yyyy-mm-dd format
    '''
    df = pd.DataFrame()
    for dt in dts:
        assert len(dt) == 10, 'Dates must be in yyyy-mm-dd format.'
        try:
            df = pd.concat([df, read_file(dt)], ignore_index=True)
        except:
            pass  # file does not exist
    return df

def clean(df):
    '''
    Add unique identifiers columns as well as turnstile count per station
    (tuid, buid, suid, ts_count).

    '''
    # Remove whitespaces from columns with string values
    str_cols = ['c_a', 'unit', 'scp', 'station', 'linename', 'division', 'desc']
    for col in str_cols:
        df[col] = df[col].str.strip()

    # TODO: sort linename

    # TODO: NaN handling. Rows with empty cells or '-' 

    # Create UID to uniquely identify a turnstile by (c_a, unit, scp, station)
    df['tuid'] = pd.factorize(df['c_a'] + df['unit'] + df['scp'] + df['station'])[0]
    # Create UID to uniquely identify a station by (station, linename)
    df['suid'] = pd.factorize(df['station'] + df['linename'])[0]
    # Create UID to uniquely identify an operator booth by
    # (c_a, station, linename)
    df['buid'] = pd.factorize(df['c_a'] + df['unit'] + \
                        df['station'] + df['linename'])[0]
    
    # Sort by [suid, tuid, datetime]
    # This ensures that when we later groupby either tuid or suid, 
    # rows within each group will appear chronologically
    df = df.sort_values(['suid','tuid','datetime'])
    # Reindex df to reflect the new sorting
    df = df.reset_index(drop=True) # drop=True gets rid of old index

    # Instead of adding ts_count to each row in the original df,
    # wouldn't it be better to create a dictionary to map
    # {suid: ts_count} ?

    df_ss = df.groupby('suid')['tuid'].nunique().to_dict()
    df['ts_count'] = df.suid.map(df_ss)
    return df


def calc_nets(df):
    '''
    Create two new columns (net_entries, net_exits) that contains the net 
    entries and net exits of each turnstile for each four hour period.
    AKA converts entries and exits from cumulative values to net values.

    '''
    # Group by tuid and calculate deltas between rows
    # This assumes df is sorted by ['tuid', 'datetime']
    # Else, we'll get incorrect deltas

    if 'net_entries' in df.columns and 'net_exits' in df.columns:
        return df  # calc_nets has already been run

    tuid_groups = df.groupby(['tuid'])
    df['net_entries'] = tuid_groups['entries'].diff().shift(-1)
    df['net_exits'] = tuid_groups['exits'].diff().shift(-1)

    # TODO: Note that this leaves the last row of each group with NaN values
    # for net_entries and net_exits. Handle that by dropping them
    df = df.dropna()

    # TODO: Convert np.float64 columns to np.int64 using .astype(int)
    df['net_entries'] = df['net_entries'].astype(int)
    df['net_exits'] = df['net_exits'].astype(int)

    # Handle ridiculously large net_entries and net_exits
    # Some net_entries and net_exits are < 0, e.g. a turnstile that counts
    # backards. Drop those rows
    threshold = 7200  #  more than 1 person every 2 secs is unlikely
    df = (df[(df['net_entries']>=0) & (df['net_exits']>=0) &
            (df['net_entries']<=threshold) & (df['net_exits']<=threshold)])


    # Create foot traffic column
    df['traffic'] = df['net_entries'] + df['net_exits']
    return df

def query_dates(df, start, end):
    """
    Args:
        df (DataFrame): preprocessed DataFrame
        start (str): start date
        end (str): end date
    """
    return df[(df['datetime'].dt.date >= pd.to_datetime(start)) & \
                (df['datetime'].dt.date < pd.to_datetime(end))]

def drop_dates(df, start, end):
    return df[(df['datetime'].dt.date < pd.to_datetime(start)) | \
                (df['datetime'].dt.date >= pd.to_datetime(end))]


def add_metrics(df):
    """
    TODO: Add in Kelsey's add_metrics.py here
    """

    pass

def get_saturdays_between(start, end):
    """
    Returns list of dates of all Saturdays between start
    and end, inclusive.

    Args:
        start (str): date in yymmdd or yyyy-mm-dd format
        end (str): date in yymmdd or yyyy-mm-dd format

    Returns:
        List of string dates in %Y-%m-%d format.
    """
    def chunk_date(dt):
        assert isinstance(dt, date) or len(dt) in [6,10]
        y = m = d = None
        if isinstance(dt, str):
            y, m, d = ((int('20'+dt[:2]), int(dt[2:4]), int(dt[4:6]))
                            if len(dt) == 6 else 
                                (int(dt[:4]), int(dt[5:7]), int(dt[8:10])))
        else:
            y, m, d = dt.year, dt.month, dt.day
        return y, m, d

    dates = []

    s_year, s_month, s_day = chunk_date(start)
    e_year, e_month, e_day = chunk_date(end)

    start = date(s_year, s_month, s_day)
    end = date(e_year, e_month, e_day)

    # Saturday is +5 on datetime's weekday() calendar.
    # Add the difference between 5 and start.weekday()
    # to get to the nearest Saturday. Then add another
    # 7 days and mod that by 7 to get the closest 
    # Saturday in the future
    s_offset = (12 - start.weekday()) % 7
    # Whatever day of the week it is, go to the nearest
    # Monday, which is +0 on datetime's weekday() calendar.
    # Subtract an extra 2 days to get to a Saturday in 
    # the past, then mod by 7 to get the nearest 
    # Saturday in the past
    e_offset = (end.weekday() + 2) % 7

    start += timedelta(days=s_offset)
    end -= timedelta(days=e_offset)

    curr = start
    while curr <= end:
        dates.append(curr.strftime('%Y-%m-%d'))
        curr += timedelta(days=7)
    return dates

def run(dname='200627', ename='', data_dir='./mta_data/'):
    '''
    Executes the main cleaning code, calling other functions to clean up data
    add various columns for sorting and interpret cumulative ENTRIES and EXITS
    columns in NET_ENTRIES and NET_EXITS columns.

    Optional arguments:
    dname -- Saturday of week desired. If ename is also given, dname is
    Saturday of first week desired
    ename -- Saturday of last week desired if more than one is desired

    '''
    if ename:
        # get list of datestrings between dname and ename
        dates = get_saturdays_between(dname, ename)
        df = read_files(dates)
    else:
        # reading only one file
        assert len(dname) in [6,10]
        dname = ('20{}-{}-{}'.format(dname[:2], dname[2:4], dname[4:6]) if 
                    len(dname) == 6 else dname)
        df = read_file(dname)

    df = calc_nets(clean(df))
    return df

def agg_by(df, *args):
    '''
    Aggregate the net entries and exits columns by date, station, or both.
    Input must be a data frame that has already been processed by the run()
    function!

    Possible arguments:
    'date' -- aggregates entry and exit data for each full day
    'time' -- aggregates entry and exit data for each four hour chunk
    'day' -- aggregates entry and exit data by day of week (will only
        really be useful if data contains >1 week)
    'week/end' -- aggregates entry and exit data into week (M-F) and weekend
    'station' -- aggregates entry and exit data for each station
    'booth' -- aggregates entry and exit data for each booth

    '''

    aggs = ['datetime', 'tuid']
    if 'booth' in args:
        aggs[1] = 'buid'
    elif 'station' in args:
        aggs[1] = 'suid'

    if 'date' in args:
        aggs = [aggs[1], df['datetime'].dt.date.rename('date')]
    elif 'time' in args:
        aggs = [aggs[1], df['datetime'].dt.time.rename('time')]

    if 'day' in args:
        agg3 = df['datetime'].dt.day_name().rename('day')
        aggs = [agg3, *aggs]
    elif 'week/end' in args:
        aggs = [df['datetime'].dt.dayofweek.apply(lambda x: 'weekend'
                                                   if  x >= 5 else 'week')\
                                                   .rename('week/end'), *aggs]

    # Raise error if none of the args given were recognized
    if aggs == ['datetime', 'tuid']:
        raise ValueError('Incorrect input argument(s)')


    df = df.groupby(aggs)[['net_entries', 'net_exits']].sum().reset_index()
    return df
