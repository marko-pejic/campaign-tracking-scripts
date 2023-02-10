
import json
import os
import subprocess
from impression_beacon import get_impression_beacons
from s3_impression_files_list import list_s3_bucket, download_events
from athena import get_beacon_html

def get_collection(year, month):
    file_name = year + '_' + month + ".json"
    if file_name in os.listdir('.'):
        with open(file_name, 'r') as f:
            return json.load(f)
    else:
        return list_s3_bucket(year, month)
    
def get_covered_days(month, year):
    files = os.listdir('.')
    covered_days = list()
    for day in range(1, 31):
        if day < 10:
            day_str = f'0{day}'
        else:
            day_str = str(day)
        if f"{month}_{day_str}_impression_beacons.json" in files:
            covered_days.append(day_str)
    return covered_days


YEAR = '2022'
MONTH = '10'

collection = get_collection(YEAR, MONTH) # list_s3_bucket(YEAR, MONTH)

# beaconsToFindHtml = []
# impressionBeacons = {}


covered_days = get_covered_days(MONTH, YEAR)

for day in collection:
    print(f"Day is {day}")
    if day in covered_days:
        continue
    """
        with open(f"{MONTH}_{day}_impression_beacons.json", "r") as f:
            impressionBeacons = json.load(f)
        
        with open(f"{MONTH}_{day}_beacons.json", "r") as f:
            beaconsToFindHtml = json.load(f)
        continue"""
    beaconsToFindHtml = []
    impressionBeacons = {}
    
    if 'beacons' not in collection[day]:
        continue
    
    impressions = collection[day]['impressions']
    beacons = collection[day]['beacons']

    for impression in impressions:
        print('Looking up impression id ' + impression)
        storedBeacons = get_impression_beacons(impression)
        print('Found beaconds ' + str(storedBeacons))
        print('-------------------------------------')

        if impression not in impressionBeacons:
            impressionBeacons[impression] = []

        for storedBeacon in storedBeacons:
            if storedBeacon in beacons:
                print('Related beacons confirmed ' + storedBeacon)

                impressionBeacons[impression].append(storedBeacon)
                beaconsToFindHtml.append(storedBeacon)
            else:
                print('Related beacons missing ' + storedBeacon)
        print('-------------------------------------')
    f = open(f"{MONTH}_{day}_impression_beacons.json", "w")
    f.write(json.dumps(impressionBeacons))
    f.close()
    
    f = open(f"{MONTH}_{day}_beacons.json", "w")
    f.write(json.dumps(beaconsToFindHtml))
    f.close()

"""f = open(MONTH + "_impression_beacons.json", "w")
f.write(json.dumps(impressionBeacons))
f.close()

f = open(MONTH + "_beacons.json", "w")
f.write(json.dumps(beaconsToFindHtml))
f.close()


beaconsAthenaList = ''
for b in beaconsToFindHtml:
    beaconsAthenaList += "'" + b + "'" + ','

queryResults = get_beacon_html(beaconsAthenaList[:-1], YEAR, MONTH)

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
                    
                    day = ''
                    for d in collection:
                        if imp in collection[d]['impressions']:
                            day = d

                    print('DAY')
                    print(day)

  
                    if not os.path.exists(f'data/{YEAR}/{MONTH}/' + day + '/' + imp):
                        os.makedirs(f'data/{YEAR}/{MONTH}/' + day + '/' + imp)
                    
                    f = open(f'data/{YEAR}/{MONTH}/' + day + '/' + imp + '/' + beaconId + ".html", "w")
                    f.write(html)
                    f.close()"""













