import os
import csv
import pandas as pd
from pathlib import Path

print("\033[H\033[J") # Clears terminal window in vs code

def get_data(file):
    df = pd.read_csv(file, keep_default_na=False)
    df.fillna('')
    data = df.to_dict(orient='records')
    return data

DM_DATA = Path.cwd() / "data" / "input"  / "dm.csv"
print("Reading",DM_DATA)
assert DM_DATA.exists(), f"DM_DATA not found"
all_dm_data = get_data(DM_DATA)

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

OUTPUT_FILE = OUTPUT_PATH / "enrolment.csv"
print("Saving to",OUTPUT_FILE)
with open(OUTPUT_FILE, 'w') as csvfile:
    # creating a csv dict writer object
    writer = csv.DictWriter(csvfile, fieldnames=output_variables)

    # writing headers (field names)
    writer.writeheader()

    # writing data rows
    writer.writerows(subjects)
print("Done")
