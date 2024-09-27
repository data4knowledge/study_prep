import json
import csv
import pandas as pd
from pathlib import Path

def get_xpt_data(file):
    df = pd.read_sas(file,format='xport',encoding='ISO-8859-1')
    df = df.fillna('')
    data = df.to_dict(orient='records')
    return data

IMPORT_PATH = Path('/Users/johannes/Library/CloudStorage/OneDrive-data4knowledge/shared_mac/standards/metadata-submission-guidelines/SDTM-MSG_v2.0_Sample_Submission_Package/m5/datasets/cdiscpilot01/tabulations/sdtm/')
OUTPUT_PATH = Path('/Users/johannes/Library/CloudStorage/OneDrive-data4knowledge/shared_mac/pilots/data/msg')

def reduce_dm():
    FULL_DM_DATA = IMPORT_PATH / "dm.xpt"
    print("Reading",FULL_DM_DATA)
    assert FULL_DM_DATA.exists(), "FULL_DM_DATA not found"

    dm = get_xpt_data(FULL_DM_DATA)
    print("full len(dm)",len(dm))

    # ['STUDYID', 'DOMAIN', 'USUBJID', 'SUBJID', 'RFSTDTC', 'RFENDTC', 'RFXSTDTC', 'RFXENDTC', 'RFICDTC', 'RFPENDTC', 'DTHDTC', 'DTHFL', 'SITEID', 'BRTHDTC', 'AGE', 'AGEU', 'SEX', 'RACE', 'ETHNIC', 'ARMCD', 'ARM', 'ACTARMCD', 'ACTARM', 'ARMNRS', 'ACTARMUD', 'COUNTRY']
    dm_data = []
    for row in dm[0:10]:
        dm_data.append(row)

    print("reduced len(dm_data)",len(dm_data))

    DM_JSON = Path.cwd() / "data" / "input" / "msg" / "dm.json"
    print("Saving",DM_JSON)
    with open(DM_JSON, 'w') as f:
        f.write(json.dumps(dm_data, indent = 2))
    print("done")

    DM_CSV = OUTPUT_PATH / "dm.csv"
    df = pd.DataFrame(dm_data)
    df.to_csv(DM_CSV, index = False)
    print("Saving",DM_CSV)

    DM_XLSX = OUTPUT_PATH / "dm.xlsx"
    df = pd.DataFrame(dm_data)
    df.to_excel(DM_XLSX, index = False)
    print("Saving",DM_XLSX)

if __name__ == "__main__":
    reduce_dm()
