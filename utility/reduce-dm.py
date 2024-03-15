import json
import csv
from pathlib import Path

def get_data(file):
    with open(file, mode ='r')as file:
        csv_file = csv.DictReader(file)
        data = list(csv_file)
    if len(list(data[0].keys())) < 3:
        print("I don't think the csv was read properly")
    return data
    # with open(file, 'r') as file:
    #     header = file.readline().strip().split("\t")
    #     data = []
    #     for line in file.readlines():
    #         values = line.strip().split("\t")
    #         x = dict(map(lambda i,j : (i,j) , header,values))
    #         data.append(x)
    # return data

DM_DATA = Path.cwd() / "tmp" / "dm-full.csv"
assert DM_DATA.exists(), "DM_DATA not found"

dm = get_data(DM_DATA)

subjects = [x['USUBJID'] for x in dm][0:10]
print("subjects",subjects)

exit()

VS_DATA_FULL = Path.cwd() / "data" / "vs-full.tsv"
assert VS_DATA_FULL.exists(), "SDTM_DATA not found"
vs = get_data(VS_DATA_FULL)
print("len(vs)",len(vs))
vs_data = [x for x in vs if x['USUBJID'] in subjects and x['VSTEST'] in LABELS]
print("len(vs_data)",len(vs_data))

VS_DATA = Path.cwd() / "data" / "vs.json"
with open(VS_DATA, 'w') as f:
    f.write(json.dumps(vs_data, indent = 2))

VS_DATA = Path.cwd() / "data" / "vs.tsv"
with open(VS_DATA, 'w') as file:
    i = 0
    for x in vs_data:
        if i == 0:
            s = '\t'.join(list(x.keys()))
            file.write(s)
            file.write("\n")
        s = '\t'.join(list(x.values()))
        file.write(s)
        file.write("\n")
        i = i + 1

