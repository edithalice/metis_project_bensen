"""
Combines percent_daily and density_plotter functions into a single module for plotting

Functions: 
- density_barplot(df, timeframe): barplot of df
- density_traffic_plot(df, timeframe): scatterplot of density vs traffic 
- density_traffic_dist(df, timeframe, var): seaborn displot of density OR traffic

pct_de Functions: 
- pct_dist(): draws distplot of pct_de
- pct_plot(): draws plot of how many stations capture what pct_de (e.g. top 50 stations -> 38%$ of daily entries)


"""

import density_plotter as dp
import percent_daily as pct

def density_barplot(df, timeframe): 
    dp.density_barplot(df, timeframe)

def density_traffic_plot(df, timeframe):
    dp.density_traffic_plot(df, timeframe)

def density_traffic_dist(df, timeframe, var): 
    dp.density_traffic_dist(df, timeframe, var)

def pct_dist(df, timeframe): 
    pct.pct_dist(df, timeframe)

def pct_plot(df, timeframe): 
    pct.pct_plot(df, timeframe)

