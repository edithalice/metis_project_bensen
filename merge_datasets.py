'''
Module to create a data frame from the remote complex data and merge it into
the main data frame. Can also create a data frame from the spatial data set
and merge it into the previous data set.

Call merge_complex(df) to create and merge in the complex data set.
Call merge_spatial(df) to create and merge in the spatial data set.
Unfortunately, I haven't yet worked out how to merge it in more accurately, so
it will only be locationally accurate if you groupby complex_id and aggregate
the data you need.
Call spt() if you just want to pull up a clean data frame of
the spatial data set

'''

import numpy as np
import pandas as pd

SPT_COLUMNS = {'Station ID': 'stid_spt',
               'Complex ID': 'complex_id',
               'GTFS Stop ID': 'stop_id',
               'Division': 'division_spt',
               'Line': 'route',
               'Stop Name': 'stop',
               'Borough': 'borough',
               'Daytime Routes': 'linename_spt',
               'Structure': 'structure',
               'GTFS Latitude': 'latitude',
               'GTFS Longitude': 'longitude',
               'North Direction Label': 'north_label',
               'South Direction Label': 'south_label'}


def merge_complex(df):
    '''
    Create and clean a data frame from the remote complex data set and merge
    into main turnstile data.

    '''
    key = pd.read_csv('remote-complex-lookup.csv')
    key['line_name'] = key['line_name'].apply(lambda x:''.join(sorted(x)))
    key['complex_id'] = key['complex_id'].astype(int)
    new_df = df.merge(key, left_on=['unit', 'c_a'],
                      right_on=['remote', 'booth'])
    return new_df

def spt():
    '''
    Create and clean spt data frame from spatial data set
    '''
    spt = pd.read_csv('http://web.mta.info/developers/data/nyct/subway/Stations.csv')
    spt = spt.rename(columns=SPT_COLUMNS)
    return spt

def merge_spt(df):
    '''
    Create and merge the spatial data set with the main turnstile data set.

    '''
    spt = spt()
    if 'complex_id' not in df.columns:
        df = merge_complex(df)

    # Don't like this merge :( it works but if the mapping from new_df to spt
    # is inaccurate for anything other complex_id
    new_df = df.merge(spt, on='complex_id')
    return new_df
