import json
import pandas as pd
from pathlib import Path

def get_xpt_data(file):
    df = pd.read_sas(file,format='xport',encoding='ISO-8859-1')
    df = df.fillna('')
    data = df.to_dict(orient='records')
    return data

def reduce_ex():
    LABELS = [
        "Height",
        "Pulse Rate",
        "Diastolic Blood Pressure",
        "Systolic Blood Pressure",
        "Weight",
        "Temperature",
    ]
 
    DM_DATA = Path.cwd() / "data" / "input" / "dm.json"
    print("Reading DM_DATA:",DM_DATA)
    assert DM_DATA.exists(), "DM_DATA not found"
    with open(DM_DATA) as f:
        dm = json.load(f)

    subjects = [x['USUBJID'] for x in dm]
    print("subjects",subjects)

    EX_DATA_FULL = Path.cwd() / "tmp" / "ex.xpt"
    assert EX_DATA_FULL.exists(), "EX_DATA_FULL not found"
    ex = get_xpt_data(EX_DATA_FULL)

    print("full len(ex)",len(ex))
    ex_data = [x for x in ex if x['USUBJID'] in subjects]
    print("reduced len(ex_data)",len(ex_data))

    EX_DATA = Path.cwd() / "data" / "input" / "ex.json"
    with open(EX_DATA, 'w') as f:
        f.write(json.dumps(ex_data, indent = 2))

    EX_CSV = Path.cwd() / "tmp" / "ex.csv"
    df = pd.DataFrame(ex_data)
    df.to_csv(EX_CSV, index = False)


    print("Done")

if __name__ == "__main__":
    reduce_ex()
