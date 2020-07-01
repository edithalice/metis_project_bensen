'''
Module for wrangling data.

You'll want to use two main functions:
- call run() to return a clean data frame with net entry and exit data calculated.
- call agg_by(df, args) to return a data frame with entry and exit data summed 
  according to args. Possible args are 'date', 'station', or both

'''

import numpy as np
import pandas as pd

# Deals with SettingWithCopyWarning
pd.options.mode.chained_assignment = None

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


def read_file(date, data_dir='./mta_data/'):
    '''
    Assumes data files are in ./mta_data/ directory
    Args:
        date (str): yyyy-mm-dd format date
    '''
    try:
        df = pd.read_csv(data_dir+'turnstile_{}.txt'.format(
                                            date[2:4]+date[5:7]+date[8:10]), 
                                            parse_dates=[['DATE', 'TIME']])
        df.columns = list(map((lambda x: x.strip() if isinstance(x, str) else x), 
                      df.columns.values))

        df = df.rename(columns=COLUMNS)
        return df
    except:
        # file does not exist
        return None

def read_files(dates, data_dir='./mta_data/'):
    '''
    Reads multiple files and returns one single DataFrame

    Args:
        dates (list): list of dates in yyyy-mm-dd format
    '''
    df = pd.DataFrame()
    for dt in dates:
        assert len(dt) == 10, 'Dates must be in yyyy-mm-dd format.'
        try:
            df = pd.concat([df, pd.read_csv(data_dir+'turnstile_{}.txt' \
                                            .format(dt[2:4]+dt[5:7]+dt[8:10]), \
                                            parse_dates=[['DATE', 'TIME']])])

            df.columns = list(map((lambda x: x.strip() if isinstance(x, str) else x), 
                  df.columns.values))

            df = df.rename(columns=COLUMNS)
            return df
        except:
            # file does not exist
            return None

def clean(df):
    '''
    Add new columns (date, tuid, station) to ease grouping and add unique identifiers.

    '''
    # TODO: Remove whitespaces from columns with string values
    str_cols = ['c_a', 'unit', 'scp', 'station', 'linename', 'division', 'desc']
    for col in str_cols:
        df[col] = df[col].str.strip()

    # TODO: NaN handling. Rows with empty cells or '-' 

    # Create UID to uniquely identify a turnstile by (c_a, unit, scp, station)
    df['tuid'] = pd.factorize(df['c_a'] + df['unit'] + df['scp'] + df['station'])[0]
    # Create UID to uniquely identify a station by (station, linename)
    df['suid'] = pd.factorize(df['station'] + df['linename'])[0]
    # Sort by [suid, tuid, datetime]
    # This ensures that when we later groupby either TUID or SUID, 
    # rows within each group will appear chronologically
    df = df.sort_values(['suid','tuid','datetime'])
    # Reindex df to reflect the new sorting
    df = df.reset_index(drop=True) # drop=True gets rid of old index
    return df

def calc_nets(df):
    '''
    Create two new columns (net_entries, net_exits) that contains the net 
    entries and net exits of each turnstile for each four hour period.
    AKA converts entries and exits from cumulative values to net values.

    '''
    # Group by TUID and calculate deltas between rows
    # This assumes df is sorted by ['TUID', 'DATETIME']
    # Else, we'll get incorrect deltas
    tuid_groups = df.groupby(['tuid'])
    df['net_entries'] = tuid_groups['entries'].diff().shift(-1)
    df['net_exits'] = tuid_groups['exits'].diff().shift(-1)

    # TODO: Note that this leaves the last row of each group with NaN values
    # for net_entries and net_exits. Handle that by dropping them
    df = df.dropna()

    # TODO: Convert np.float64 columns to np.int64 using .astype(int)
    df['net_entries'] = df['net_entries'].astype(int)
    df['net_exits'] = df['net_exits'].astype(int)

    # Some net_entries and net_exits are < 0, e.g. a turnstil that counts
    # backards. Handle that
    df = df[(df['net_entries']>=0) & (df['net_exits']>=0)]

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
    agg_cols = ['net_entries','net_exits']
    if 'date' in args and 'station' in args:
        df = df.groupby([df['suid'], df['datetime'].dt.date])[agg_cols].sum()
        df.reset_index(inplace=True)
    elif 'station' in args:
        df = df.groupby(['datetime', 'suid'])[agg_cols].sum()
        df.reset_index(inplace=True)
    elif 'date' in args:
        df = df.groupby([df['tuid'], df['datetime'].dt.date])[agg_cols].sum()
        df.reset_index(inplace=True)
    else: raise ValueError('Incorrect input argument')
    return df

def run():
    '''
    Executes the main cleaning code, calling other functions to clean up data and
    add date, net_entries and net_exits columns.

    '''
    # print (read_files(['2020-06-27','2020-06-27']))
    # df = read_file('2020-06-27')
    df = read_files(['2020-06-27', '2020-06-20'])
    df = clean(df)
    df = calc_nets(df)
    print (agg_by(df, 'date', 'station'))
    print (df)
    return df

run()
