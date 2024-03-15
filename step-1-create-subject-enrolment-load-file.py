import os
import csv
from pathlib import Path

print("\033[H\033[J") # Clears terminal window in vs code

def get_data(file):
    with open(file, mode ='r')as file:
        csv_file = csv.DictReader(file)
        data = list(csv_file)
    if len(list(data[0].keys())) < 3:
        print("I don't think the csv was read properly")
    return data

DM_DATA = Path.cwd() / "data" / "dm.csv"
assert DM_DATA.exists(), "DM_DATA not found"
print("Reading",DM_DATA)
dm_data = get_data(DM_DATA)

OUTPUT_PATH = Path.cwd() / "data" / "output"
assert OUTPUT_PATH.exists(), "OUTPUT_PATH not found"

# Get a few subjects and reduce to the needed variables
n_subjects = 10
print("Reducing number of subjects to",n_subjects)
subjects = []
output_variables = ['STUDY_URI','SITEID','USUBJID']
for row in dm_data[:n_subjects]:
    row['STUDY_URI'] = "https://study.d4k.dk/study-cdisc-pilot-lzzt"
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
