"""
Provides functions for plotting density data

Functions: 
- clean_df: preps df for plotters and adds density column
- density_barplot(df, timeframe): barplot of df
- density_traffic_plot(df, timeframe): scatterplot of density vs traffic 
- density_traffic_dist(df, timeframe, var): seaborn displot of density OR traffic
"""

import pandas as pd
import numpy as np
import get_metrics as gm
import wrangle_data as wd
import seaborn as sns
import matplotlib
from matplotlib import pyplot as plt

def clean_df(df):
    """
    Cleans DataFrame for plotting functions and adds density column
    """
    df_temp = df.copy()
    # add density column
    df_temp = gm.density(df, add_col=True)
    # convert to date
    df_temp['datetime'] = df_temp['datetime'].dt.date
    # get the total density by day
    df_temp = df_temp.groupby(['suid', 'datetime']).sum().reset_index().sort_values(['datetime', 'density'], ascending=[True,False])
    # get the mean for the entire timeframe  
    df_temp = df_temp.groupby('suid').mean().sort_values('density', ascending=False).reset_index()
    
    return df_temp

def density_barplot(df, timeframe):
    """
    creates a barplot of density by station, ordered largest to smallest

    args:
    timeframe -> str: timeframe title for plot
    """
    df_temp = clean_df(df)

    x = df_temp['suid']
    y = df_temp['density']
    plt.figure(figsize=(20,10))
    plt.xlabel('Station', fontsize=15)
    plt.ylabel('Density\n (Traffic/Turnstile)', fontsize=15)
    plt.title('Station Density\n {}'.format(timeframe), fontsize=20, weight='bold')
    plt.tick_params(axis='x', which='both', bottom=False, labelbottom=False)
    plt.grid()
    sns.barplot(x,y, order=df_temp.sort_values('density', ascending=False).suid)

def density_traffic_plot(df, timeframe):
    """
    generates a scatterplot of density vs traffic
        *traffic is displayed in log scale

    args: 
    df -> DataFrame from wd.run()
    timeframe -> str: timeframe title for plot
    """
    df_temp = clean_df(df)
    
    x = df_temp['traffic']
    y = df_temp['density']

    fig, ax = plt.subplots(figsize=(20,10))
    ax.scatter(x, y)
    ax.set_title('Traffic vs Density\n {}'.format(timeframe), fontsize=20, weight='bold')
    ax.set_xlabel('Mean Daily Traffic\n (Entries + Exits)\n Log Scale', fontsize=15)
    ax.set_ylabel('Mean Density\n (Traffic/Turnstile)', fontsize=15)
    ax.grid(axis='y')
    ax.set_xscale('log')
    ax.set_xticks([100, 500, 1000, 2000, 5000, 10000, 20000, 50000, 100000, 250000, 500000])
    ax.get_xaxis().set_major_formatter(matplotlib.ticker.ScalarFormatter())

def density_traffic_dist(df, timeframe, var):
    """
    plots distribution of either density or traffic data

    args: 
    df -> DataFrame from wd.run()
    timeframe -> str: timeframe for graph title
    type -> str: 'density' or 'traffic' -> variable for distribution
    """
    
    df_temp = clean_df(df)[['suid', 'traffic', 'density']].set_index('suid')

    plt.figure(figsize=(10,10))
    plt.title('Distribution of Station {}\n {}'.format(var.capitalize(), timeframe), fontsize=20, weight='bold')
    plt.xlabel('Density\n (Traffic/Turnstile)')

    sns.distplot(df_temp[var], bins=20, hist_kws=dict(edgecolor='r', linewidth=1))