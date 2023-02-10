import json
import os
import math
from athena import get_beacon_html


def write_results(queryResults, impressionBeacons, day):
    for results in queryResults['ResultSet']['Rows']:
        if results['Data'][0]['VarCharValue'] != 'beacon_id':
            beaconId = results['Data'][0]['VarCharValue']
            html = results['Data'][2]['VarCharValue']
            for imp in impressionBeacons:
                for bcn in impressionBeacons[imp]:
                    if bcn == beaconId:
                        print('IMPRESSION')
                        print(imp)
                        print('BEACON')
                        print(beaconId)

                        # day = ''
                        # for d in collection:
                        #    if imp in collection[d]['impressions']:
                        #        day = d

                        print('DAY')
                        print(day)


                        if not os.path.exists(f'data/{YEAR}/{MONTH}/' + day_str + '/' + imp):
                            os.makedirs(f'data/{YEAR}/{MONTH}/' + day_str + '/' + imp)

                        f = open(f'data/{YEAR}/{MONTH}/' + day_str + '/' + imp + '/' + beaconId + ".html", "w")
                        f.write(html)
                        f.close()

MONTH = '10'
YEAR = '2022'

beaconsToFindHtml = list()
impressionBeacons = dict()
files = os.listdir('.')

for day in range(29, 32):
    if day < 10:
        day_str = f'0{day}'
    else:
        day_str = str(day)
        
    if f"{MONTH}_{day_str}_beacons.json" in files:
        with open(f"{MONTH}_{day_str}_beacons.json", "r") as f:
            beaconsToFindHtml = json.load(f)
            
        with open(f"{MONTH}_{day_str}_impression_beacons.json", "r") as f:
            impressionBeacons = json.load(f)

        
    # break it down into more, because of query length
    
    num_in_batch = 3000
    if len(beaconsToFindHtml) < 3000:
        num_in_batch = len(beaconsToFindHtml)
        if num_in_batch == 0:
            num_in_batch = 1
    
    num_batches = math.ceil(len(beaconsToFindHtml)/num_in_batch)
    # TODO: calculate batch numbers based on number of beacons
    for batch in range(1, num_batches+1):
        remaining_beacons = beaconsToFindHtml[(batch-1)*num_in_batch:batch*num_in_batch]
        if len(remaining_beacons) == 0:
            break
        beaconsAthenaList = ''
        for b in remaining_beacons:
            beaconsAthenaList += "'" + b + "'" + ','

        queryResults = get_beacon_html(beaconsAthenaList[:-1], YEAR, MONTH)
        write_results(queryResults, impressionBeacons, day)
    
