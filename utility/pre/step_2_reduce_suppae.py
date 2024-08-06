import json
import pandas as pd
from pathlib import Path

def get_xpt_data(file):
    df = pd.read_sas(file,format='xport',encoding='ISO-8859-1')
    df = df.fillna('')
    data = df.to_dict(orient='records')
    return data

def reduce_suppae():
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

    SUPPAE_DATA_FULL = Path.cwd() / "tmp" / "suppae.xpt"
    assert SUPPAE_DATA_FULL.exists(), "SUPPAE_DATA_FULL not found"
    suppae = get_xpt_data(SUPPAE_DATA_FULL)

    print("full len(suppae)",len(suppae))
    suppae_data = [x for x in suppae if x['USUBJID'] in subjects]
    print("reduced len(suppae_data)",len(suppae_data))

    AE_DATA = Path.cwd() / "data" / "input" / "suppae.json"
    with open(AE_DATA, 'w') as f:
        f.write(json.dumps(suppae_data, indent = 2))

    AE_CSV = Path.cwd() / "tmp" / "suppae.csv"
    df = pd.DataFrame(suppae_data)
    df.to_csv(AE_CSV, index = False)


    print("Done")

if __name__ == "__main__":
    reduce_suppae()
