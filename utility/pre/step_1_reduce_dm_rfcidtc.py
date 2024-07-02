import json
import csv
import pandas as pd
from pathlib import Path

# print("\033[H\033[J") # Clears terminal window in vs code

def get_xpt_data(file):
    df = pd.read_sas(file,format='xport',encoding='ISO-8859-1')
    df = df.fillna('')
    data = df.to_dict(orient='records')
    return data

def reduce_dm_core():
    FULL_DM_DATA = Path.cwd() / "tmp" / "dm.xpt"
    print("Reading",FULL_DM_DATA)
    assert FULL_DM_DATA.exists(), "FULL_DM_DATA not found"

    # CORE_DM_DATA = Path.cwd() / "tmp" / "dm_core.xpt"
    # print("Reading",CORE_DM_DATA)
    # assert CORE_DM_DATA.exists(), "CORE_DM_DATA not found"

    dm = get_xpt_data(FULL_DM_DATA)
    print("full len(dm)",len(dm))

    # dm_core = get_xpt_data(CORE_DM_DATA)
    # print("full len(dm_core)",len(dm_core))

    # dm_data = [x for x in dm[0:10]]
    # dm_data = [x for x in dm if dm['RFICDTC'] != ""]
    dm_data = []
    for row in dm:
        # print("row.__class__",row.__class__)
        # print("row['RFICDTC']",row['RFICDTC'])
        new_usubjid = row['USUBJID']
        print("usubjid",row['USUBJID'],new_usubjid)
        # row['USUBJID'] = row['USUBJID']
        dm_data.append(row)
    print("reduced len(dm_data)",len(dm_data))

    # DM_JSON = Path.cwd() / "data" / "input" / "dm.json"
    # print("Saving",DM_JSON)
    # with open(DM_JSON, 'w') as f:
    #     f.write(json.dumps(dm_data, indent = 2))
    print("done")

# def reduce_ds():
#     FULL_DS_DATA = Path.cwd() / "tmp" / "ds.xpt"
#     print("Reading",FULL_DS_DATA)
#     assert FULL_DS_DATA.exists(), "FULL_DS_DATA not found"

#     ds = get_xpt_data(FULL_DS_DATA)
#     print("full len(ds)",len(ds))

#     # ds_data = [x for x in ds[0:10]]
#     # ds_data = [x for x in ds if ds['RFICDTC'] != ""]
#     ds_data = []
#     keys = ['USUBJID','DSSTDTC','DSCAT','DSDECOD']
#     ds_decodes = set()
#     for row in ds:
#         # print("row.__class__",row.__class__)
#         submap = {key: row[key] for key  in keys}
#         print("submap",submap)
#         ds_decodes.add(row['DSDECOD'])
#         # print("row['RFICDTC']",row['DSSTDTC'])
#         ds_data.append(row)
#     print("ds_decodes",ds_decodes)
#     print("reduced len(ds_data)",len(ds_data))

#     DS_JSON = Path.cwd() / "data" / "input" / "ds.json"
#     print("Saving",DS_JSON)
#     with open(DS_JSON, 'w') as f:
#         f.write(json.dumps(ds_data, indent = 2))
#     print("done")

if __name__ == "__main__":
    print("not running as main")
    # reduce_dm_core()
    # reduce_ds()

