### metis_project_bensen
# Project 1 for the Metis bootcamp
---
## Our Scope
As of May 2020, the New York MTA implemented 24/7 sanitization services and other measures intended to stem the spread of COVID-19. The MTA manages one of largest and most populated public transit systems in the world, but is fundamentally restrained by the limitations of municipal budget and manpower. 
The MTA must determine how to efficiently distribute its limited sanitization resources -- a problem which requires an understanding of how ridership and station density have changed in light of the pandemic and its consequent impact on the flow of local travel.
By analyzing and visualizing the shifting ridership density, scale, and geographic proportions over the first six months of 2020, we aim to provide clear information to increase the efficiency of the MTA’s distribution of public health resources. 

---
### Some Relevant Metis Links

[Project Description](https://github.com/thisismetis/sf20_ds19/tree/master/curriculum/project-01/project-01-introduction)

[Code Style](https://github.com/thisismetis/sf20_ds19/blob/master/curriculum/project-01/code-style/code-style.pdf)

[Presentation Tips](https://github.com/thisismetis/sf20_ds19/blob/master/curriculum/project-01/presentation-guide/PresentationGuide.pdf)

---
### Sample code to import data
```{python}
def get_data(week_nums):
    url = "http://web.mta.info/developers/data/nyct/turnstile/turnstile_{}.txt"
    dfs = []
    for week_num in week_nums:
        file_url = url.format(week_num)
        dfs.append(pd.read_csv(file_url))
    return pd.concat(dfs)
        
week_nums = [160903, 160910, 160917]
turnstiles_df = get_data(week_nums)
```
