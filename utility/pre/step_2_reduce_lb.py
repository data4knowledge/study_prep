import json
import pandas as pd
from pathlib import Path

def get_xpt_data(file):
    df = pd.read_sas(file,format='xport',encoding='ISO-8859-1')
    df = df.fillna('')
    data = df.to_dict(orient='records')
    return data

def reduce_lb():
    LABELS = [
        "Alanine Aminotransferase",
        "Hemoglobin A1C",
        "Aspartate Aminotransferase",
        "Alkaline Phosphatase",
    ]
        # Ignoring to reduce size 
        # "Creatinine",
        # "Potassium",
        # "Sodium"

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

if __name__ == "__main__":
    reduce_lb()
