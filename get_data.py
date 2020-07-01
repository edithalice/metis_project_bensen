import os
import sys
import requests
from datetime import date, datetime, timedelta

def get_most_recent_saturday():
    today = date.today()
    # today.weekday() is 0-indexed starting on Monday (e.g. Thu is 3)
    # to get most recent Saturday, we first get nearest Monday,
    # then we subtract 2 extra days
    offset = today.weekday()+2
    last_sat = today - timedelta(days=offset)
    return last_sat

def get_saturdays_after(dt, last_sat):
    """
    Returns list of dates of all Saturdays from dt to last_sat
    """
    dates = []
    cutoff = datetime.strptime(dt, '%Y-%m-%d').date()

    curr = last_sat
    while curr > cutoff:
        dates.append(curr.strftime('%Y-%m-%d'))
        curr = curr - timedelta(days=7)
    return dates

def get_data(dates):
    # check if directory mta_data exists
    if not os.path.isdir('./mta_data'):
        # make mta_data
        os.mkdir('./mta_data')
    # convert dates to list of filenames
    filenames = list(map(lambda x: 'turnstile_{}{}{}.txt'.format(x[2:4], x[5:7], x[8:10]), dates))

    for f in filenames:
        if os.path.isfile('./mta_data/{}'.format(f)):
            continue
        print ('Downloading... {}'.format(f))
        url = 'http://web.mta.info/developers/data/nyct/turnstile/{}'.format(f)
        # download txt file from url
        r = requests.get(url)
        with open('./mta_data/{}'.format(f), 'wb') as txtfile:
            txtfile.write(r.content)

def main(argv):
    # find most recent saturday
    last_sat = get_most_recent_saturday()
    first_date = '2017-01-01' if len(argv) == 0 else argv[0]
    assert len(first_date) == 10
    dates = get_saturdays_after(first_date, last_sat)
    get_data(dates)

if __name__ == '__main__':
    main(sys.argv[1:])