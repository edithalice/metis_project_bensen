'''
Module for wrangling data.

You'll want to use two main functions:
- call run() to return a clean data frame with net entry and exit data calculated.
- call agg_by(df, args) to return a data frame with entry and exit data summed according to args. Possible args are 'date', 'station', or both

'''

import numpy as np
import pandas as pd

def clean(df):
    '''
    Add new columns (DATE, TUID, STATION) to ease grouping and add unique identifiers.

    '''
    # Create date column to make grouping by date easier
    df['DATE'] = df['DATE_TIME'].dt.date
    # Create UID to uniquely identify a turnstile by (C/A, UNIT, SCP, STATION)
    df['TUID'] = pd.factorize(df['C/A'] + df['UNIT'] + df['SCP'] + df['STATION'])[0]
    # Create UID to uniquely identify a station by(STATION, LINENAME)
    df['SUID'] = pd.factorize(df['STATION'] + df['LINENAME'])[0]
    return df

def calc_nets(df):
    '''
    Create two new columns (NET_ENTRIES, NET_EXITS) that contains the net entries and net exits of each turnstile for each four hour period.
    AKA converts ENTRIES and EXITS from cumulative values to net values.

    '''
    # Group by TUID and DATE_TIME and calculate deltas between rows
    tuid_groups = df.groupby(['TUID'])
    df['NET_ENTRIES'] = tuid_groups['ENTRIES'].diff().shift(-1)
    df['NET_EXITS'] = tuid_groups['EXITS'].diff().shift(-1)
    return df

def run():
    '''
    Executes the main cleaning code, calling other functions to clean up data and add DATE, NET_ENTRIES and NET_EXITS columns.

    '''
    data_dir = './mta_data/'
    dname = '200627'
    df = pd.read_csv(data_dir+'turnstile_'+dname+'.txt', parse_dates=[['DATE', 'TIME']])
    df.columns = list(map((lambda x: x.strip() if isinstance(x, str) else x), df.columns.values))
    df = clean(df)
    df = calc_nets(df)
    return df

def agg_by(df, *args):
    '''
    Aggregate the net entries and exits columns by date, station, or both.
    Input must be a data frame that has already been processed by the run() function!

    Possible arguments:
    'date' -- aggregates entry and exit data for each full day for each turnstile
    'station' -- aggregates entry and exit data for each station for each four hour time stamp
    'date', 'station' -- aggregates entry and exit data for each full day for each station

    '''
    if 'date' in args and 'station' in args:
        df = df.groupby(['SUID', 'DATE']).sum()
        df.reset_index(inplace=True)
    elif 'station' in args:
        df = df.groupby(['DATE_TIME', 'SUID']).sum()
        df.reset_index(inplace=True)
    elif 'date' in args:
        df = df.groupby(['TUID', 'DATE']).sum()
        df.reset_index(inplace=True)
    else: raise ValueError('Incorrect input argument')
    return df
