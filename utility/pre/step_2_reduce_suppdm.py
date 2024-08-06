import json
import pandas as pd
from pathlib import Path

def get_xpt_data(file):
    df = pd.read_sas(file,format='xport',encoding='ISO-8859-1')
    df = df.fillna('')
    data = df.to_dict(orient='records')
    return data

def reduce_suppdm():
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

    SUPPDM_DATA_FULL = Path.cwd() / "tmp" / "suppdm.xpt"
    assert SUPPDM_DATA_FULL.exists(), "SUPPDM_DATA_FULL not found"
    suppdm = get_xpt_data(SUPPDM_DATA_FULL)

    print("full len(suppdm)",len(suppdm))
    suppdm_data = [x for x in suppdm if x['USUBJID'] in subjects]
    print("reduced len(suppdm_data)",len(suppdm_data))

    SUPPDM_DATA = Path.cwd() / "data" / "input" / "suppdm.json"
    with open(SUPPDM_DATA, 'w') as f:
        f.write(json.dumps(suppdm_data, indent = 2))

    SUPPDM_CSV = Path.cwd() / "tmp" / "suppdm.csv"
    df = pd.DataFrame(suppdm_data)
    df.to_csv(SUPPDM_CSV, index = False)


    print("Done")

if __name__ == "__main__":
    reduce_suppdm()
