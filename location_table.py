# Written By: Kelvin Abrokwa-Johnson
# Improve By: Miranda Lv
# Purpose: Produce level 0 location product
# Input: Level_0/locations.tsv

import pandas
import urllib2
import json
from time import sleep
import sys
#from datetime import datetime

#reload(sys)
#sys.setdefaultencoding('utf-8')

loc=r"/home/mirandalv/repos/minerva-geocoded-dataset/iraq/output/level0/sample.csv"

data = pandas.read_csv(loc, sep='\t') # Locations Level 0 file
newdata = dict()
newtitle = []
gid = list( data.GEONAME_ID )
place_names = [] # Will be populated with place names directly from GeoNames API
latls = []
lngls = []
loctypels = []
countadmcodels = []
countadmnamels = []

    
def main():
  count = 0
  for i in gid:
    count += 1
    valid_place_name = False
    sleep(0.6)

    url = "http://api.geonames.org/hierarchyJSON?geonameId=" + str(i) + "&username=jpowell"

    error_count = 0

    while valid_place_name == False:

      try:
		  
		  response = urllib2.urlopen(url).read()
		  json_result = json.loads(response)
		  place = json_result['geonames'][-1]['name']
		  lat = json_result['geonames'][-1]['lat']
		  lng = json_result['geonames'][-1]['lng']
		  loctypecode = json_result['geonames'][-1]['fcode'] #PPL
		  countrycode = json_result['geonames'][-1]['countryCode'] #NP
		  countryname = json_result['geonames'][-1]['countryName'] # Nepal
		  admcode1 = json_result['geonames'][-1]['adminCode1'] #00
		  admname1 = json_result['geonames'][-1]['adminName1']
		  
		  if admname1 == "":
			  countadmname = countryname
		  else:
			  countadmname = countryname + "|" + admname1
			  
		  if admcode1 == "":
			  countadmcode = countrycode
		  else:
			  countadmcode = countrycode + "|" + admcode1
			  
		  valid_place_name = True
        #print(str(datetime.now())) # To keep track of time taken per request

      except ValueError: # TODO: tweak to catch specific errors (probably KeyError and ValueError)
        error_count += 1
        print "ERROR, Retrying..."

      except KeyError:
        error_count += 1
        print "ERROR, Retrying..."

      if error_count > 50:
        print "No results for: ", i
        place = "ERROR: Not Found"
        break


    place_names.append(place)
    latls.append(lat)
    lngls.append(lng)
    loctypels.append(loctypecode)
    countadmcodels.append(countadmcode)
    countadmnamels.append(countadmname)
    #admcode1ls.append(admcode1)
    #admname1ls.append(admname1)

    #print str(i) + " : " + place
    print str(i)
    print "line number:", count
    print "url: ", url
    print '-'*30
  
  
  newtitle = ['project_id', 'geoname_id', 'precision_code', 'place_name', 'latitude', 'longitude', 'location_type_code', 'geoname_adm_code', 'geonames_adm_name']
  newdata['project_id'] = data.PROJECT_ID
  newdata['geoname_id'] = data.GEONAME_ID	
  newdata['precision_code'] = data.PRECISION_CODE
  newdata['place_name'] = place_names
  newdata['latitude'] = latls
  newdata['longitude'] = lngls
  newdata['location_type_code'] = loctypels
  newdata['geoname_adm_code'] = countadmcodels
  newdata['geonames_adm_name'] = countadmnamels
  df = pandas.DataFrame.from_dict(newdata)

  df.to_csv('locations_iraq.tsv', sep='\t', columns=newtitle, header=newtitle, encoding='utf-8', index=False)

  print "SUCCESS"


if __name__ == "__main__":
  main()
