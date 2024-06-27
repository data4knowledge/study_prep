import json
import csv
import pandas as pd
from pathlib import Path

def create_subject_enrolment_load_file():
    DM_DATA = Path.cwd() / "data" / "input"  / "dm.json"
    print("Reading",DM_DATA)
    assert DM_DATA.exists(), f"DM_DATA not found"
    with open(DM_DATA) as f:
        all_dm_data = json.load(f)

    OUTPUT_PATH = Path.cwd() / "data" / "output"
    assert OUTPUT_PATH.exists(), "OUTPUT_PATH not found"

    # Get a few subjects and reduce to the needed variables
    n_subjects = 10
    print("Reducing number of subjects to",n_subjects)
    dm_data = all_dm_data[0:n_subjects]

    # Add STUDY_URI
    for row in dm_data:
        row['STUDY_URI'] = "https://study.d4k.dk/study-cdisc-pilot-lzzt"

    subjects = []
    output_variables = ['STUDY_URI','SITEID','USUBJID']

    for row in dm_data[:n_subjects]:
        subjects.append(dict((k, row[k]) for k in output_variables))

    def save_file(path: Path, name):
        OUTPUT_FILE = path / f"{name}.json"
        print("Saving to",OUTPUT_FILE)
        with open(OUTPUT_FILE, 'w') as f:
            f.write(json.dumps(subjects, indent = 2))

        OUTPUT_FILE = path / f"{name}.csv"
        print("Saving to",OUTPUT_FILE)
        with open(OUTPUT_FILE, 'w') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=output_variables)
            writer.writeheader()
            writer.writerows(subjects)
        
    save_file(OUTPUT_PATH,"enrolment")
    print("Done")

if __name__ == "__main__":
    create_subject_enrolment_load_file()
