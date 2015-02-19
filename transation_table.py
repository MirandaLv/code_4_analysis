import pandas as pd
import json
import sys
import csv
from pandas import DataFrame as df

inf = r"/home/mirandalv/repos/nepal-geocoded-dataset/scratch1/transation/ComDis_NP-csv.tsv"
csvoutput = r"/home/mirandalv/repos/nepal-geocoded-dataset/scratch1/transation/transation_NP.csv"

def get_fields(incsv):
	rawData = csv.DictReader(open(inf, 'rb'), delimiter='\t')
	header=rawData.fieldnames
	return header

rawData = csv.DictReader(open(inf, 'rb'), delimiter='\t')
fields = rawData.fieldnames
c_dfields = fields[2:]

newheader = ["project_id", "project_title", "transaction_year", "transaction_value_code", "transaction_value"]


indicate = []

for row in rawData:
	for field in c_dfields:	
		if str(row[field]) != str(0):
			newrow = {}
			trans_val_code = field.split("_")[0]
			trans_year = field.split("_")[1]
			newrow["project_id"] = row["AMP ID"]
			newrow["transaction_original_date"] = ""
			newrow["transaction_isodate"] = ""
			newrow["transaction_year"] = trans_year
			newrow["transaction_value_code"] = trans_val_code
			newrow["transaction_value"] = row[field]
			indicate.append(newrow)



outdf=df(indicate, columns=newheader)
outdf.to_csv(csvoutput, sep='\t', quotechar='\"', encoding='utf-8')





