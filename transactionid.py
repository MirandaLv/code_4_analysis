# Create a UUID for transaction table 
# Name: Miranda Lv
# Date: 2/16/2015

import uuid
from pandas import DataFrame as df
import pandas as pd

inf = r"/home/mirandalv/repos/minerva-geocoded-dataset/iraq/output/transactions.tsv"
outf = r"/home/mirandalv/repos/minerva-geocoded-dataset/iraq/output/transactions_id.tsv"

data = pd.read_csv(inf, sep='\t')

idlist = list(data.project_id)

transactionid = []

for i in idlist:
	tranid = uuid.uuid4()
	transactionid.append(tranid)

data['transaction_id'] = transactionid

data.to_csv(outf, sep='\t', encoding='utf-8', index=False)


