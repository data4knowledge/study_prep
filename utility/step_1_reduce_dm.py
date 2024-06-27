import json
import csv
import pandas as pd
from pathlib import Path

print("\033[H\033[J") # Clears terminal window in vs code

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

    dm_data = [x for x in dm[0:10]]
    print("reduced len(dm_data)",len(dm_data))

    DM_JSON = Path.cwd() / "data" / "input" / "dm.json"
    print("Saving",DM_JSON)
    with open(DM_JSON, 'w') as f:
        f.write(json.dumps(dm_data, indent = 2))
    print("done")

if __name__ == "__main__":
    print("not running as main")
    # reduce_dm()
