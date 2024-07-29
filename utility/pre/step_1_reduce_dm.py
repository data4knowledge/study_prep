import json
import csv
import pandas as pd
from pathlib import Path

def get_xpt_data(file):
    df = pd.read_sas(file,format='xport',encoding='ISO-8859-1')
    df = df.fillna('')
    data = df.to_dict(orient='records')
    return data


def reduce_dm():
    FULL_DM_DATA = Path.cwd() / "tmp" / "dm.xpt"
    print("Reading",FULL_DM_DATA)
    assert FULL_DM_DATA.exists(), "FULL_DM_DATA not found"

    dm = get_xpt_data(FULL_DM_DATA)
    print("full len(dm)",len(dm))

    birth_dates = [
        '1980-06-30',
        '1978-03-01',
        '1985-03-01',
        '1970-05-24',
        '1978-07-13',
        '1983-06-13',
        '1982-10-13',
        '1982-10-13',
        '1967-12-10',
        '1972-11-22',
    ]

    # dm_data = [x for x in dm[0:10]]
    dm_data = []
    print("N.B! Faking 1) RFICDTC from DMDTC 2) BRTHDTC is hardcoded")
    i = 0
    for row in dm[0:10]:
        row['RFICDTC'] = row['DMDTC']
        if i < len(birth_dates):
            row['BRTHDTC'] = birth_dates[i]
        else:
            row['BRTHDTC'] = '1980-01-01'
        dm_data.append(row)
        i += 1

    print("reduced len(dm_data)",len(dm_data))

    DM_JSON = Path.cwd() / "data" / "input" / "dm.json"
    print("Saving",DM_JSON)
    with open(DM_JSON, 'w') as f:
        f.write(json.dumps(dm_data, indent = 2))
    print("done")

if __name__ == "__main__":
    reduce_dm()
