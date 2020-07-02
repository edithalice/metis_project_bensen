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


def read_file(date, data_dir='./mta_data/'):
    '''
    Assumes data files are in ./mta_data/ directory
    Args:
        date (str): yyyy-mm-dd format date
    '''
    assert isinstance(date, str), 'Date must be in yymmdd or yyyy-mm-dd format.'
    assert len(date) in [6,10]
    dname = date if len(date) == 6 else date[2:4]+date[5:7]+date[8:10]
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
            df = pd.concat([df, read_file(dt)], ignore_index=True)
        except:
            pass  # file does not exist
    return df

def clean(df):
    '''
    Add new columns (date, tuid, station) to ease grouping and add unique identifiers.

    '''
    # TODO: Remove whitespaces from columns with string values
    str_cols = ['c_a', 'unit', 'scp', 'station', 'linename', 'division', 'desc']
    for col in str_cols:
        df[col] = df[col].str.strip()

    # TODO: NaN handling. Rows with empty cells or '-' 

    # Create date column to make grouping by date easier
    df['date'] = df['datetime'].dt.date
    # Create time column to make grouping by time easier
    df['time'] = df['datetime'].dt.time
    # Create UID to uniquely identify a turnstile by (c_a, unit, scp, station)
    df['tuid'] = pd.factorize(df['c_a'] + df['unit'] + df['scp'] + df['station'])[0]
    # Create UID to uniquely identify a station by (station, linename)
    df['suid'] = pd.factorize(df['station'] + df['linename'])[0]
    # Create UID to uniquely identify an operator booth by
    # (c_a, station, linename)
    df['buid'] = pd.factorize(df['c_a'] + df['station'] + df['linename'])[0]

    ## Creating a column to add a turnstile count for each suid
    # ran into issues with repeating TS_COUNT across rows for each suid
    # hopefully going to make functional soon
    # df['STATION_SIZE'] = df.groupby('suid')['tuid']\
    # .nunique().reset_index().rename(columns={'tuid':'TS_COUNT'})
    
    # Sort by [suid, tuid, datetime]
    # This ensures that when we later groupby either tuid or suid, 
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

    # TODO: Handle ridiculously large net_entries and net_exits
    # Some net_entries and net_exits are < 0, e.g. a turnstile that counts
    # backards. Drop those rows
    threshold = 7200  #  more than 1 person every 2 secs is unlikely
    df = (df[(df['net_entries']>=0) & (df['net_exits']>=0) &
            (df['net_entries']<=threshold) & (df['net_exits']<=threshold)])
    return df


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

    # go to future monday and subtract 2
    s_offset = (12 - start.weekday()) % 7
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
    'station' -- aggregates entry and exit data for each station
    'booth' -- aggregates entry and exit data for each booth

    '''

    time_agg = 'datetime'
    spatial_agg = 'tuid'

    if 'date' in args:
        time_agg = 'date'
    elif 'time' in args:
        time_agg = 'time'

    if 'booth' in args:
        spatial_agg = 'buid'
    elif 'station' in args:
        spatial_agg = 'suid'

    # Raise error if none of the args given were recognized
    if time_agg == 'datetime' and spatial_agg == 'tuid':
        raise ValueError('Incorrect input argument(s)')

    # if aggregating by date or time with or without other factors, must group
    # by date/time after the spatial factor. However, if not aggregating by
    # date or time at all, must group by date_time prior to spatial factor
    if time_agg == 'datetime':
        agg1, agg2 = time_agg, spatial_agg
    else:
        agg1, agg2 = spatial_agg, time_agg

    df = df.groupby([agg1, agg2])\
                    [['net_entries', 'net_exits']].sum().reset_index()
    return df
