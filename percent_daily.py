import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd
import numpy as np
import get_metrics as gm

def clean_df(df):
    """
    Add pct_de column and clean for plotting/top_station functions
    """
    df_temp = gm.pct_daily_entries(df)
    df_temp['date'] = df_temp['date'].dt.date
    df_temp = df_temp.groupby(['date', 'suid', 'tde']).agg('sum').reset_index().sort_values(['date','pct_de'], ascending=[True,False])
    df_temp = df_temp.reset_index().drop(columns='index').groupby('suid').mean().sort_values('pct_de', ascending=False)
    df_temp['pct_de'] = df_temp['pct_de'] * 100
    return df_temp

def pct_dist(df, timeframe):
    """
    draws distplot of pct_de 
    
    arguments: 
    df -> dataframe pulled from wd.run()
    timeframe = string: name of time period to be printed on title
    """
    df_temp = clean_df(df)
    
    plt.figure(figsize=(10,5))
    plt.title("Percent of Total Entries\n Distribution\n" + '{}'.format(timeframe) , fontsize=30)
    # + '{}'.format(timeframe)
    plt.xlabel('Percent of Total Daily Entries', fontsize=20)
    plt.ylabel('# of Stations', fontsize=20)
    plt.grid()
    plt.yticks(np.linspace(0, 5, 6))
    plt.xticks(np.linspace(0, 10, 101))
    sns.distplot(df_temp['pct_de'], bins=20)

def pct_plot(df, timeframe):
    """
    plots top_stations df (defined below)
    
    args: 
    timeframe = string: name of time period to be printed on title
    """
    df_temp = clean_df(df)
    
    pcts = []
    mask = df_temp['pct_de']
    for x in range(0, len(df_temp)):
        pcts.append(['# of Stations: {}'.format(x), mask[0:x].sum()])
        
    focus = [400, 300, 200, 150, 100, 50, 25, 10]
    stations = [pcts[i] for i in range(len(pcts))]
    x = [pct[0] for pct in stations]
    y = [pct[1] for pct in stations]
    
    plt.figure(figsize=(10,10))
    plt.title("% of daily entries in N stations\n" + '{}'.format(timeframe), fontsize=30)
    plt.xlabel('# of Stations', fontsize=20)
    plt.ylabel('% of Total Entries', fontsize=20)
    plt.grid()
    plt.yticks(np.linspace(0, 100, 10))
    plt.xticks(np.linspace(0, 500, 6))
    for i in focus:
        plt.annotate(s=stations[i][0][-3:] + ' Stations\n' + str(round(stations[i][1], 1)) + '%', xy=(i, stations[i][1]), xytext=(i+2, stations[i][1]-10), fontsize=13, weight='bold', arrowprops=dict(arrowstyle='wedge'))
    plt.plot(x,y, linewidth=7)
   
   
def top_stations(df, multi=True, date='2019-03-09', plot=False, time_title=''):
    """
    Returns a list [pct] of lists [x,y] 
    where x = the number of stations observed
    and y = the percentage of total entries they represent
    aggregated by day and averaged across df's time period
      
    arguments:
    df -> DataFrame pulled from wd.run()
    plot -> plots pct instead of returning
    time_title (required if plot=True) -> string to pass as date range for plot title e.g. 'Mar-Jun 2019'
    ///(optional)///
    multi=True -> df is in standard form - multiple days, includes datetime (function will aggregate them)
          False -> df covers a single day with multiple times
    date='mm-dd-yyyy' -> specify to view data averaged for the specified date 
    """
    df_temp = clean_df(df)
    mask = pd.DataFrame
    if multi: 
        mask = df_temp['pct_de'] 
    else:
        mask = df_temp[df_temp['date'] == pd.to_datetime(date)]['pct_de'] 
    
    pcts = []
    for x in range(0, len(df_temp)):
        pcts.append(['# of Stations: {}'.format(x), mask[0:x].sum()])
    
#     if plot: 
#         pct_plot(pcts, time_title)
#     else:
#         return pcts
    return pcts