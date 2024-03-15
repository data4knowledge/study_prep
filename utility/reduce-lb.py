import json
import pandas as pd
from pathlib import Path

LABELS = [
    "Alanine Aminotransferase",
    "Albumin",
    "Alkaline Phosphatase",
]
    # Ignoring to reduce size 
    # "Aspartate Aminotransferase",
    # "Creatinine",
    # "Potassium",
    # "Sodium"

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

LB_DATA_FULL = Path.cwd() / "tmp" / "lb.xpt"
assert LB_DATA_FULL.exists(), "LB_DATA_FULL not found"
lb = get_xpt_data(LB_DATA_FULL)

print("full len(lb)",len(lb))
lb_data = [x for x in lb if x['USUBJID'] in subjects and x['LBTEST'] in LABELS]
print("reduced len(lb_data)",len(lb_data))

LB_DATA = Path.cwd() / "data" / "input" / "lb.json"
with open(LB_DATA, 'w') as f:
    f.write(json.dumps(lb_data, indent = 2))

print("done")