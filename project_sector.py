# Date: 1/2X/2015
# Author: Miranda Lv
# Note: produce sector_type for project table release

# Nepal format

import pandas as pd
import json
import sys
import csv
from pandas import DataFrame as df
import json

inf = r"/home/mirandalv/repos/minerva-geocoded-dataset/nigeria/scratch/nigeria_sector.csv"

outf = r"/home/mirandalv/repos/minerva-geocoded-dataset/nigeria/scratch/nigeria_sector.tsv"

data = pd.read_csv(inf, sep='\t')

rawData = csv.DictReader(open(inf, 'rb'), delimiter='\t')
fields = rawData.fieldnames

sectorlist = []
for row in rawData:
	sublist = []
	if "|" in row["Sector"]:
		num = row["Sector"].count("|")
		newnum = num +1
		for i in range(0, newnum):
			dic = {}
			sector = row["Sector"].split("|")[i]
			dic["sector"] = sector
			dic["subsector"] = row["Focus Area"]
			dic["subsubsector"] = ""
			sublist.append(dic)
		sublist = json.dumps(sublist)
		sectorlist.append(sublist)
	else:
		dic = {}
		dic["sector"] = row["Sector"]
		dic["subsector"] = row["Focus Area"]
		dic["subsubsector"] = ""
		sublist.append(dic)
		sublist = json.dumps(sublist)
		sectorlist.append(sublist)



data["sector_value"] = sectorlist
data.to_csv(outf, sep='\t', index=False) #encoding='utf-8'
