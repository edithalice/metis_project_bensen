"""
Provides methods for obtaining metrics as unique DataFrames or adding the metric as a column

Current functions:

total_daily_entries(df, add_col=False) -> total entries summed across all stations by date

pct_daily_entries -> each station's summed entries proportion of total entries for the day

density -> traffic / number of turnstiles (currently by 4-hour interval)

"""

import wrangle_data as wd
import pandas as pd

"""
TO WRITE IF NOTHING ELSE TO DO
convert 2 df cols into a dict and map to second df 
   -> generalize the mapping process in total_daily_entries
"""
# def map_by_date(col): 
#     temp_df = df.copy()
    
#     colname = 
#     col_dict = col.set_index('date').to_dict()[col]
#     temp_df['datetime'] = temp_df['datetime'].dt.date
#     temp_df[colname] = 
    

def total_daily_entries(df, add_col=False):
    """
    return total entries at each station by day
    """
    df_tde = wd.agg_by(df, 'date', 'station')
    tde = df_tde[['date', 'net_entries']].groupby('date').sum().reset_index().rename(columns={'net_entries': 'tde'})
    
    # map TDE onto the df
    if add_col:
        tde_dict = tde.set_index('date').to_dict()['tde']
        df['temp_date'] = df['datetime'].dt.date
        df['tde'] = df['temp_date'].map(tde_dict)
        df.drop('temp_date', axis=1, inplace=True)
        return df
    
    # return tde as df of dates and totals
    else:
        return tde


def pct_daily_entries(df):
    """
    get station's proportion of all entries by day: 'PCT_DE'
    returns suid, date, tde, net_entries, pct_de, aggregated by suid then date
    """
    temp_df = df.copy()
    total_daily_entries(temp_df, add_col=True)
    
    # calculate pct_de: net entries / total entries
    temp_df['pct_de'] = temp_df['net_entries'] / temp_df['tde']
     
    # condense to essential columns
    pct_de = temp_df[['datetime', 'suid', 'net_entries', 'tde', 'pct_de']].groupby(['suid','datetime', 'tde']).sum().reset_index().rename(columns={'datetime':'date'})

    return pct_de


def density(df, add_col=False):
    """
    get traffic per turnstile of a station: 'DENSITY'
    date format (if 'date' column or 'datetime' column): 
    
    add_col=True -> add density column to df
    add_col=False -> returns suid, datetime, density as a df
    """
    density_df = df.copy()
    density = df['traffic'] / df['ts_count']
    if add_col:
        density_df['density'] = density
        return density_df
    else: 
        density_df = df[['datetime', 'suid', 'traffic', 'ts_count']]
        density_df['density'] = density_df['traffic'] / density_df['ts_count'] 
        return density_df
            
       