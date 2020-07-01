#Testing wrangle

import wrangle_data as wd

df = wd.run()
print(df.head(5))

df2 = wd.agg_by(df,'date')
print(df2.head(5))

df3 = wd.agg_by(df,'station')
print(df3.head(5))

df4 = wd.agg_by(df,'date', 'station')
print(df4.head(5))
