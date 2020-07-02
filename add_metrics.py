import pandas as pd
import wrangle_data as wd

df = wd.run()

def add_metrics(df):
    """
    Takes a wrangled DataFrame and adds new columns: 
    'TDE': Total Daily Entries (sum of all entries at all stations on that date)
    'TS_COUNT': Turnstile Count (per station)
    'PCT_DE': Proportion of Total Daily Entries for the station
    'DENSITY': Station's [(net entries) / (# of turnstiles)] during datetime window
    'WKDY': Day of the week
    """
    # Add total entries at each station by day: 'TDE'
    df_tde = wd.agg_by(df, 'date', 'station')
    TOTAL_DAILY_ENTRIES = df_tde[['DATE', 'NET_ENTRIES']].groupby('DATE').sum().reset_index().rename(columns={'NET_ENTRIES': 'TDE'})
    df = pd.merge(df, TOTAL_DAILY_ENTRIES, how='left')

    # Add turnstiles per station: 'TS_COUNT'
    TURNSTILE_COUNT = df.groupby('SUID')['TUID'].nunique().reset_index().rename(columns={'TUID':'TS_COUNT'})
    df = pd.merge(df, TURNSTILE_COUNT, how='left')
    
    # Add station's proportion of all entries by day: 'PCT_DE'
    df['PCT_DE'] = df['NET_ENTRIES'] / df['TDE']

    # Add net entries per turnstile by station: 'DENSITY'
    df['DENSITY'] = df['NET_ENTRIES'] / df['TS_COUNT']

    # Add day of the week
    df['WKDY'] = pd.to_datetime(df['DATE']).dt.day_name()
    
    return df