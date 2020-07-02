#Testing wrangle

import wrangle_data as wd

df = wd.run()
print(df.head(2))

df2 = wd.agg_by(df,'date')
print(df2.head(2))

df2 = wd.agg_by(df,'time')
print(df2.head(2))

df2 = wd.agg_by(df,'booth')
print(df2.head(2))

df2 = wd.agg_by(df,'station')
print(df2.head(2))

df2 = wd.agg_by(df,'date', 'booth')
print(df2.head(2))

df2 = wd.agg_by(df,'time', 'booth')
print(df2.head(2))

df2 = wd.agg_by(df,'date', 'station')
print(df2.head(2))

df2 = wd.agg_by(df,'time', 'station')
print(df2.head(2))
