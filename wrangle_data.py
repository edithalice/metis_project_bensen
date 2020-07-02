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

'''

import numpy as np
import pandas as pd
import get_data as gd

def clean(df):
    '''
    Add new columns (DATE, TIME, TUID, BUID, SUID) to ease grouping and add
    unique identifiers.

    '''

    ## Splitting DATE_TIME
    # Create date column to make grouping by date easier
    df['DATE'] = df['DATE_TIME'].dt.date
    # Create time column to make grouping by time easier
    df['TIME'] = df['DATE_TIME'].dt.time

    ## Creating unique spatial IDs (turnstile, booth, station)
    # Create UID to uniquely identify a turnstile by
    # (C/A, UNIT, SCP, STATION, LINENAME)
    df['TUID'] = pd.factorize(df['C/A'] + df['UNIT'] + df['SCP'] +
                              df['STATION'] + df['LINENAME'])[0]
    # Create UID to uniquely identify an operator booth by
    # (C/A, STATION, LINENAME)
    df['BUID'] = pd.factorize(df['C/A'] + df['STATION'] + df['LINENAME'])[0]
    # Create UID to uniquely identify a station by(STATION, LINENAME)
    df['SUID'] = pd.factorize(df['STATION'] + df['LINENAME'])[0]

    ## Creating a column to add a turnstile count for each SUID
    # ran into issues with repeating TS_COUNT across rows for each SUID
    # hopefully going to make functional soon
    # df['STATION_SIZE'] = df.groupby('SUID')['TUID']\
    # .nunique().reset_index().rename(columns={'TUID':'TS_COUNT'})

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
        dates = gd.get_saturdays_after(dname, ename)
        for date in dates[1:]:
            df.concat(pd.read_csv(data_dir+'turnstile_'+date+'.txt',
                                  parse_dates=[['DATE', 'TIME']]))
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
    'station' -- aggregates entry and exit data for each station
    'booth' -- aggregates entry and exit data for each booth

    '''

    time_agg = 'DATE_TIME'
    spatial_agg = 'TUID'

    if 'date' in args:
        time_agg = 'DATE'
    elif 'time' in args:
        time_agg = 'TIME'

    if 'booth' in args:
        spatial_agg = 'BUID'
    elif 'station' in args:
        spatial_agg = 'SUID'

    # Raise error if none of the args given were recognized
    if time_agg == 'DATE_TIME' and spatial_agg == 'TUID':
        raise ValueError('Incorrect input argument(s)')

    # if aggregating by date or time with or without other factors, must group
    # by date/time after the spatial factor. However, if not aggregating by
    # date or time at all, must group by date_time prior to spatial factor
    if time_agg == 'DATE_TIME':
        agg1, agg2 = time_agg, spatial_agg
    else:
        agg1, agg2 = spatial_agg, time_agg

    df = df.groupby([agg1, agg2])\
                    [['NET_ENTRIES', 'NET_EXITS']].sum().reset_index()
    return df
