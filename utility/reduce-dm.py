import json
import csv
import pandas as pd
from pathlib import Path

def get_data(file):
    df = pd.read_csv(file, keep_default_na=False)
    df.fillna('')
    data = df.to_dict(orient='records')
    return data

FULL_DM_DATA = Path.cwd() / "tmp" / "dm-full.csv"
assert FULL_DM_DATA.exists(), "FULL_DM_DATA not found"

dm = get_data(FULL_DM_DATA)

# subjects = [x['USUBJID'] for x in dm][0:10]
dm_data = [x for x in dm][0:10]
print("len(dm_data)",len(dm_data))

DM_JSON = Path.cwd() / "tmp" / "dm.json"
with open(DM_JSON, 'w') as f:
    f.write(json.dumps(dm_data, indent = 2))

DM_CSV = Path.cwd() / "data" / "input" / "dm.csv"
print("Saving to",DM_CSV)
output_variables=list(dm_data[0].keys())
with open(DM_CSV, 'w') as csv_file:
    writer = csv.DictWriter(csv_file, fieldnames=output_variables)
    writer.writeheader()
    writer.writerows(dm_data)
print("Done")


exit()

VS_DATA_FULL = Path.cwd() / "data" / "vs-full.tsv"
assert VS_DATA_FULL.exists(), "SDTM_DATA not found"
vs = get_data(VS_DATA_FULL)
print("len(vs)",len(vs))
vs_data = [x for x in vs if x['USUBJID'] in subjects and x['VSTEST'] in LABELS]
print("len(vs_data)",len(vs_data))


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

