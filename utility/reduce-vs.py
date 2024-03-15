import json
import csv
import pandas as pd
from pathlib import Path

LABELS = [
    "Height",
    "Pulse Rate",
    "Diastolic Blood Pressure",
    "Systolic Blood Pressure",
    "Weight",
    "Temperature",
]

def get_data(file):
    df = pd.read_csv(file, keep_default_na=False)
    df.fillna('')
    data = df.to_dict(orient='records')
    if len(list(data[0].keys())) < 3:
        print("I don't think the csv was read properly")
    return data
    with open(file, mode ='r')as file:
        csv_file = csv.DictReader(file)
        data = list(csv_file)
    return data
    # with open(file, 'r') as file:
    #     header = file.readline().strip().split("\t")
    #     data = []
    #     for line in file.readlines():
    #         values = line.strip().split("\t")
    #         x = dict(map(lambda i,j : (i,j) , header,values))
    #         data.append(x)
    # return data

DM_DATA = Path.cwd() / "data" / "input" / "dm.csv"
print("Reading DM_DATA:",DM_DATA)
assert DM_DATA.exists(), "DM_DATA not found"
dm = get_data(DM_DATA)

# subjects = [x['USUBJID'] for x in dm][0:10]
subjects = [x['USUBJID'] for x in dm]
print("subjects",subjects)

VS_DATA_FULL = Path.cwd() / "tmp" / "vs-full.csv"
assert VS_DATA_FULL.exists(), "VS_DATA_FULL not found"
vs = get_data(VS_DATA_FULL)
print("len(vs)",len(vs))
vs_data = [x for x in vs if x['USUBJID'] in subjects and x['VSTEST'] in LABELS]
print("len(vs_data)",len(vs_data))


VS_DATA = Path.cwd() / "tmp" / "vs.json"
with open(VS_DATA, 'w') as f:
    f.write(json.dumps(vs_data, indent = 2))

VS_DATA = Path.cwd() / "data" / "input" / "vs.csv"
output_variables=list(vs_data[0].keys())
with open(VS_DATA, 'w') as csv_file:
    writer = csv.DictWriter(csv_file, fieldnames=output_variables)
    writer.writeheader()
    writer.writerows(vs_data)

print("Done")
