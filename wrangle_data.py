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
import get_data as gd
from datetime import date, datetime

def clean(df):
    '''
    Add unique identifiers columns as well as turnstile count per station
    (TUID, BUID, SUID, TS_COUNT).

    '''

    # Create UID to uniquely identify a turnstile by
    # (C/A, UNIT, SCP, STATION, LINENAME)
    df['TUID'] = pd.factorize(df['C/A'] + df['UNIT'] + df['SCP'] +
                              df['STATION'] + df['LINENAME'])[0]
    # Create UID to uniquely identify an operator booth by
    # (C/A, STATION, UNIT, LINENAME)
    df['BUID'] = pd.factorize(df['C/A'] + df['UNIT'] + df['STATION'] +
                              df['LINENAME'])[0]
    # Create UID to uniquely identify a station by (STATION, LINENAME)
    df['SUID'] = pd.factorize(df['STATION'] + df['LINENAME'])[0]

    ## Create a column to add a turnstile count for each SUID
    df_ss = df.groupby('SUID')['TUID'].nunique().to_dict()
    df['TS_COUNT'] = df.SUID.map(df_ss)

    return df

def calc_nets(df):
    '''
    Create two new columns (NET_ENTRIES, NET_EXITS) that contains the net
    entries and net exits of each turnstile for each four hour period.
    AKA converts ENTRIES and EXITS from cumulative values to net values.

    '''
    # Group by TUID and DATE_TIME and calculate deltas between rows
    tuid_groups = df.groupby(['TUID'])
    df['NET_ENTRIES'] = tuid_groups['ENTRIES'].diff().shift(-1)
    df['NET_EXITS'] = tuid_groups['EXITS'].diff().shift(-1)
    return df

def run(dname='200627', ename=''):
    '''
    Executes the main cleaning code, calling other functions to clean up data
    add various columns for sorting and interpret cumulative ENTRIES and EXITS
    columns in NET_ENTRIES and NET_EXITS columns.

    Optional arguments:
    dname -- Saturday of week desired. If ename is also given, dname is
    Saturday of first week desired
    ename -- Saturday of last week desired if more than one is desired

    '''
    data_dir = './mta_data/'
    df = pd.read_csv(data_dir + 'turnstile_' + dname + '.txt',
                     parse_dates=[['DATE', 'TIME']])
    if ename:
        dname = '20' + '-'.join([dname[:2], dname[2:4], dname[4:]])
        ename = datetime.strptime(ename, '%y%m%d').date()
        dates = gd.get_saturdays_after(dname, ename)
        filenames = list(map(lambda x: 'turnstile_{}{}{}.txt'.format(x[2:4], x[5:7], x[8:10]), dates))
        for file in filenames[1:]:
            df = pd.concat([df, pd.read_csv(data_dir+file, parse_dates=[['DATE', 'TIME']])], ignore_index=True)
    df.columns = list(map((lambda x: x.strip() if isinstance(x, str) else x),
                          df.columns.values))
    df = clean(df)
    df = calc_nets(df)
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

    aggs = ['DATE_TIME', 'TUID']
    if 'booth' in args:
        aggs[1] = 'BUID'
    elif 'station' in args:
        aggs[1] = 'SUID'

    if 'date' in args:
        aggs = [aggs[1], df['DATE_TIME'].dt.date.rename('DATE')]
    elif 'time' in args:
        aggs = [aggs[1], df['DATE_TIME'].dt.time.rename('TIME')]

    if 'day' in args:
        agg3 = df['DATE_TIME'].dt.day_name().rename('DAY')
        aggs = [agg3, *aggs]
    elif 'week/end' in args:
        aggs = [df['DATE_TIME'].dt.dayofweek.apply(lambda x: 'weekend'
                                                   if  x >= 5 else 'week')\
                                                   .rename('WEEK/END'), *aggs]

    # Raise error if none of the args given were recognized
    if aggs == ['DATE_TIME', 'TUID']:
        raise ValueError('Incorrect input argument(s)')


    df = df.groupby(aggs)[['NET_ENTRIES', 'NET_EXITS']].sum().reset_index()
    return df
