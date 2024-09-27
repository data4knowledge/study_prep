import json
import pandas as pd
from pathlib import Path

def get_xpt_data(file):
    df = pd.read_sas(file,format='xport',encoding='ISO-8859-1')
    df = df.fillna('')
    data = df.to_dict(orient='records')
    return data

def get_subjects():
    DM_DATA = Path.cwd() / "data" / "input" / "pilot" / "dm.json"
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

def reduce_dataset(data, subjects):
    data = [x for x in data if x['USUBJID'] in subjects]
    print("reduced len(data)",len(data))
    return data

def reduce_dataset_tests(data, subjects, test_labels, var):
    data = [x for x in data if x['USUBJID'] in subjects and x[var] in test_labels]
    print("reduced len(data)",len(data))
    return data

def save_data(data,domain):
    OUTPUT_JSON = Path.cwd() / "data" / "input"/ "pilot" / f"{domain}.json"
    with open(OUTPUT_JSON, 'w') as f:
        f.write(json.dumps(data, indent = 2))
    print("saved json: ",OUTPUT_JSON)

    OUTPUT_CSV = OUTPUT_PATH / f"{domain}.csv"
    df = pd.DataFrame(data)
    df.to_csv(OUTPUT_CSV, index = False)
    print("saved csv: ",OUTPUT_CSV)

    OUTPUT_XLSX = OUTPUT_PATH / f"{domain}.xlsx"
    df = pd.DataFrame(data)
    df.to_excel(OUTPUT_XLSX, index = False)
    print("saved xlsx: ",OUTPUT_XLSX)


def reduce_domain(subjects, domain):
    ds_data = get_dataset(domain)
    ds_data = reduce_dataset(ds_data, subjects)
    save_data(ds_data,domain)

def reduce_lb(subjects):
    domain = 'lb'
    # Ignoring to reduce size 
    # "Creatinine",
    # "Potassium",
    # "Sodium"
    test_labels = [
        "Alanine Aminotransferase",
        "Hemoglobin A1C",
        "Aspartate Aminotransferase",
        "Alkaline Phosphatase",
    ]
    print("tests",test_labels)
    data = get_dataset(domain)
    data = reduce_dataset_tests(data, subjects, test_labels, 'LBTEST')
    save_data(data,domain)

def reduce_vs(subjects):
    domain = 'vs'
    test_labels = [
        "Height",
        "Pulse Rate",
        "Diastolic Blood Pressure",
        "Systolic Blood Pressure",
        "Weight",
        "Temperature",
    ]
    print("tests",test_labels)
    data = get_dataset(domain)
    data = reduce_dataset_tests(data, subjects, test_labels, 'VSTEST')
    save_data(data,domain)

IMPORT_PATH = Path('/Users/johannes/Library/CloudStorage/OneDrive-data4knowledge/shared_mac/pilots/updated_pilot_submission_package/Updated Pilot Submission Package/900172/m5/datasets/cdiscpilot01/tabulations/sdtm')
OUTPUT_PATH = Path('/Users/johannes/Library/CloudStorage/OneDrive-data4knowledge/shared_mac/pilots/data/pilot')

if __name__ == "__main__":
    subjects = get_subjects()
    print("len(subjects)",len(subjects))
    reduce_lb(subjects)
    reduce_vs(subjects)
    reduce_domain(subjects, "ae")
    reduce_domain(subjects, "suppae")
    reduce_domain(subjects, "suppdm")
    reduce_domain(subjects, "ex")
    reduce_domain(subjects, 'ds')
    print("Done")
