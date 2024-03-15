import json
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

print("\033[H\033[J") # Clears terminal window in vs code

def get_xpt_data(file):
    df = pd.read_sas(file,format='xport',encoding='ISO-8859-1')
    df = df.fillna('')
    data = df.to_dict(orient='records')
    return data


DM_DATA = Path.cwd() / "data" / "input" / "dm.json"
print("Reading DM_DATA:",DM_DATA)
assert DM_DATA.exists(), "DM_DATA not found"
with open(DM_DATA) as f:
    dm = json.load(f)

subjects = [x['USUBJID'] for x in dm]
print("subjects",subjects)

VS_DATA_FULL = Path.cwd() / "tmp" / "vs.xpt"
assert VS_DATA_FULL.exists(), "VS_DATA_FULL not found"
vs = get_xpt_data(VS_DATA_FULL)

print("len(vs)",len(vs))
vs_data = [x for x in vs if x['USUBJID'] in subjects and x['VSTEST'] in LABELS]
print("len(vs_data)",len(vs_data))

VS_DATA = Path.cwd() / "data" / "input" / "vs.json"
with open(VS_DATA, 'w') as f:
    f.write(json.dumps(vs_data, indent = 2))

print("Done")
