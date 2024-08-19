import json
import pandas as pd
from pathlib import Path

def get_xpt_data(file):
    df = pd.read_sas(file,format='xport',encoding='ISO-8859-1')
    df = df.fillna('')
    data = df.to_dict(orient='records')
    return data

def get_subjects():
    DM_DATA = Path.cwd() / "data" / "input" / "dm.json"
    print("Reading DM_DATA:",DM_DATA)
    assert DM_DATA.exists(), "DM_DATA not found"
    with open(DM_DATA) as f:
        dm = json.load(f)

    subjects = [x['USUBJID'] for x in dm]
    print("subjects",subjects)
    return subjects

def get_dataset(domain):
    # DATA_PATH = Path.cwd() / "tmp" / f"{domain}.xpt"
    DATA_PATH = IMPORT_PATH / f"{domain}.xpt"
    assert DATA_PATH.exists(), f"DATA_PATH not found: {DATA_PATH}"
    data = get_xpt_data(DATA_PATH)
    print(f"full len({domain})",len(data))
    return data

def reduce_dataset(data, subjects, labels = None):
    if labels:
        data = [x for x in data if x['USUBJID'] in subjects and x['LBTEST'] in labels]
    else:
        data = [x for x in data if x['USUBJID'] in subjects]
    print("reduced len(data)",len(data))
    return data

def save_data(data,domain):
    OUTPUT_JSON = Path.cwd() / "data" / "input" / f"{domain}.json"
    # OUTPUT_JSON = Path.cwd() / "data" / "input" / f"{domain}.json"
    with open(OUTPUT_JSON, 'w') as f:
        f.write(json.dumps(data, indent = 2))
    print("saved json: ",OUTPUT_JSON)

    OUTPUT_CSV = OUTPUT_PATH / f"{domain}.csv"
    df = pd.DataFrame(data)
    df.to_csv(OUTPUT_CSV, index = False)
    print("saved csv: ",OUTPUT_CSV)


def reduce_ae(subjects):
    domain = 'ae'
    ae_data = get_dataset(domain)
    ae_data = reduce_dataset(ae_data, subjects)
    save_data(ae_data,domain)

def reduce_ds(subjects):
    domain = 'ds'
    ds_data = get_dataset(domain)
    ds_data = reduce_dataset(ds_data, subjects)
    save_data(ds_data,domain)

def reduce_domain(subjects):
    domain = 'ex'
    ds_data = get_dataset(domain)
    ds_data = reduce_dataset(ds_data, subjects)
    save_data(ds_data,domain)

def reduce_lb(subjects):
    domain = 'lb'
    LABELS = [
        "Alanine Aminotransferase",
        "Hemoglobin A1C",
        "Aspartate Aminotransferase",
        "Alkaline Phosphatase",
    ]
    print("tests",LABELS)
        # Ignoring to reduce size 
        # "Creatinine",
        # "Potassium",
        # "Sodium"
    data = get_dataset(domain)
    data = reduce_dataset(data, subjects)
    save_data(data,domain)

IMPORT_PATH = Path('/Users/johannes/Library/CloudStorage/OneDrive-data4knowledge/shared_mac/pilots/updated_pilot_submission_package/Updated Pilot Submission Package/900172/m5/datasets/cdiscpilot01/tabulations/sdtm')
OUTPUT_PATH = Path('/Users/johannes/Library/CloudStorage/OneDrive-data4knowledge/shared_mac/pilots')

if __name__ == "__main__":
    subjects = get_subjects()
    print("len(subjects)",len(subjects))
    # reduce_ae(subjects)
    # reduce_ds(subjects)
    reduce_lb(subjects)
    # reduce_ex(subjects)
    print("Done")
