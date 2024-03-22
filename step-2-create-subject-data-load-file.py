import json
import csv
from pathlib import Path

print("\033[H\033[J") # Clears terminal window in vs code


DATA_LABELS_TO_BC_LABELS = {
    'Temperature': 'Body Temperature',
    'Weight': 'Body Weight',
    'Height': 'Body Height',
    'Pulse Rate': 'Heart Rate',
    'Alanine Aminotransferase': 'Alanine Aminotransferase Measurement',
    'Sodium': 'Sodium Measurement',
    'Aspartate Aminotransferase': 'Aspartate Aminotransferase Measurement',
    'Potassium': 'Potassium Measurement',
    'Albumin': 'Albumin Measurement',
    'Creatinine': 'Creatinine Measurement',
    'Alkaline Phosphatase': 'Alkaline Phosphatase Measurement',
    'Diastolic Blood Pressure': 'Diastolic Blood Pressure',
    'Systolic Blood Pressure': 'Systolic Blood Pressure',
    'ALP': '',
    'ALT': '',
    'K': '',
    'ALB': '',
    'SODIUM': '',
    'AST': '',
    'CREAT': ''
}

# Unknown visits
# 'RETRIEVAL': 'CHECK', 
# 'AMBUL ECG PLACEMENT': 'CHECK', 
# 'AMBUL ECG REMOVAL': 'CHECK'
DATA_VISITS_TO_ENCOUNTER_LABELS = {
    'SCREENING 1': 'Screening 1', 
    'SCREENING 2': 'Screening 2', 
    'BASELINE': 'Baseline', 
    'WEEK 2': 'Week 2', 
    'WEEK 4': 'Week 4', 
    'WEEK 6': 'Week 6', 
    'WEEK 8': 'Week 8', 
    'WEEK 12': 'Week 12', 
    'WEEK 16': 'Week 16', 
    'WEEK 20': 'Week 20', 
    'WEEK 26': 'Week 24', 
    'WEEK 24': 'Week 26', 
}

DATA_TPT_TO_TIMING_LABELS = {
    "AFTER LYING DOWN FOR 5 MINUTES": 'PT5M',
    "AFTER STANDING FOR 1 MINUTE"   : 'PT1M',
    "AFTER STANDING FOR 3 MINUTES"  : 'PT2M'
}

# result_name = 'Vital Signs Result' # Old name
result_name = 'Clinical Test Result' # Updated name
TEST_ROW_VARIABLE_TO_BC_PROPERTY = {
    "Weight": {
        "VSORRES": result_name,
        "VSORRESU": "Unit of Weight",
    },
    "Height": {
        "VSORRES": result_name,
        "VSORRESU": "Unit of Height",
    },
    "Temperature": {
        "VSORRES": result_name,
        "VSORRESU": "Unit of Temperature",
    },
    "Diastolic Blood Pressure": {
        "VSORRES": result_name,
        "VSORRESU": "Unit of Pressure",
    },
    "Systolic Blood Pressure": {
        "VSORRES": result_name,
        "VSORRESU": "Unit of Pressure",
    },
    "Pulse Rate": {
        "VSORRES": result_name,
        "VSORRESU": "Count per Minute"
    },
}

def add_issue(*txts):
    issues.append(" ".join(txts))


def save_file(path: Path, name, data):
    OUTPUT_FILE = path / f"{name}.json"
    print("Saving to",OUTPUT_FILE)
    with open(OUTPUT_FILE, 'w') as f:
        f.write(json.dumps(data, indent = 2))

    OUTPUT_FILE = path / f"{name}.csv"
    print("Saving to",OUTPUT_FILE)
    output_variables = list(data[0].keys())
    with open(OUTPUT_FILE, 'w') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=output_variables)
        writer.writeheader()
        writer.writerows(data)


def clean(txt):
    result = ""
    if isinstance(txt, str):
        result = txt
    elif isinstance(txt, float):
        result = str(txt)
    else:
        print("clean does not now of:",txt.__class__,txt)

    return result.replace(".","/")


def get_property_for_variable(test,variable):
    property = None
    if test in TEST_ROW_VARIABLE_TO_BC_PROPERTY and variable in TEST_ROW_VARIABLE_TO_BC_PROPERTY[test]:
        property = TEST_ROW_VARIABLE_TO_BC_PROPERTY[test][variable]
    else:
        print("Add property",test,variable)
    return property

def get_encounter(row):
    encounter = "NOT SET"
    if 'VISIT' in row:
        if row['VISIT'] in DATA_VISITS_TO_ENCOUNTER_LABELS:
            encounter = DATA_VISITS_TO_ENCOUNTER_LABELS[row['VISIT']]
        else:
            encounter = ""
    else:
        encounter = f"VISIT not in row"
    return encounter

def get_bc_label(test_label):
    bc_label = ""
    if test_label in DATA_LABELS_TO_BC_LABELS: 
        bc_label = DATA_LABELS_TO_BC_LABELS[test_label]
    else:
        print("Add bc_label:",test_label)
    return bc_label

def get_data_contract(encounter,bc_label,property,tpt):
    if tpt == "":
        dc_item = next((item for item in data_contracts if item["BC_LABEL"] == bc_label and item['BCP_NAME'] == property and item['ENCOUNTER_LABEL'] == encounter), None)
    else:
        dc_item = next((item for item in data_contracts if item["BC_LABEL"] == bc_label and item['BCP_NAME'] == property and item['ENCOUNTER_LABEL'] == encounter and item['TIMEPOINT_VALUE'] == tpt), None)

    if dc_item != None:
        matches.append(dc_item)
        if "DC_URI" in dc_item:
            return dc_item['DC_URI']
        else:
            print("Missing DC_URI", encounter, bc_label)
    else:
        add_issue("Miss ENCOUNTER_LABEL:",encounter)
        return None

DM_DATA = Path.cwd() / "data" / "output" / "enrolment.json"
assert DM_DATA.exists(), "DM_DATA not found"
print("\nGetting subjects from file",DM_DATA)
with open(DM_DATA) as f:
    dm_data = json.load(f)


DATA_CONTRACTS_LOOKUP = Path.cwd() / "data" / "output" / "data_contracts.json"
assert DATA_CONTRACTS_LOOKUP.exists(), "DATA_CONTRACTS_LOOKUP not found"
print("\nGetting data contracts from file",DATA_CONTRACTS_LOOKUP)
with open(DATA_CONTRACTS_LOOKUP) as f:
    data_contracts = json.load(f)

OUTPUT_PATH = Path.cwd() / "data" / "output"
assert OUTPUT_PATH.exists(), "OUTPUT_PATH not found"

# Get subjects from the enrolment file
subjects = [row['USUBJID'] for row in dm_data]


print("\nGetting VS data")
VS_DATA = Path.cwd() / "data" / "input" / "vs.json"
assert VS_DATA.exists(), "VS_DATA not found"
with open(VS_DATA) as f:
    vs_data = json.load(f)

matches = []
issues = []

print("\nCreating datapoint and value")
data = []

for row in vs_data:
    datapoint_root = f"{row['USUBJID']}/{row['DOMAIN']}/{clean(row['VSSEQ'])}"
    item = {}

    # Result
    encounter = get_encounter(row)
    if encounter != "":
        bc_label = get_bc_label(row['VSTEST'])
        tpt = ""
        if 'VSTPT' in row and row['VSTPT'] != "":
            tpt = DATA_TPT_TO_TIMING_LABELS[row['VSTPT']]

        property = get_property_for_variable(row['VSTEST'],'VSORRES')

        data_contract = get_data_contract(encounter,bc_label,property,tpt)

        if data_contract:
            item['USUBJID'] = row['USUBJID']
            item['DC_URI'] = data_contract
            item['DATAPOINT'] = f"{datapoint_root}{row['VSTESTCD']}/result"
            item['VALUE'] = f"{row['VSORRES']}"
            data.append(item)
        else:
            add_issue(f"No dc RESULT bc_label: {bc_label} - property: {property} - encounter: {encounter}")

        # Unit
        encounter = get_encounter(row)
        bc_label = get_bc_label(row['VSTEST'])
        property = get_property_for_variable(row['VSTEST'],'VSORRESU')
        data_contract = get_data_contract(encounter,bc_label,property,tpt)
        if data_contract:
            item = {}
            item['USUBJID'] = row['USUBJID']
            item['DC_URI'] = data_contract
            item['DATAPOINT'] = f"{datapoint_root}{row['VSTESTCD']}/unit"
            item['VALUE'] = f"{row['VSORRESU']}"
            data.append(item)
        else:
            add_issue("No dc UNIT bc_label:", bc_label, "- encounter:", encounter, "property:", property)
    else:
            add_issue("Ignoring visit", row['VISIT'], "encounter:", encounter)
        

print("---Datapoint - Data contract matches:",len(matches))
print("---Non matching Datapoints (e.g. visit not defined)",len(issues))
print("\nIssues")
for issue in set(issues):
    print(issue)
print("")

if len(data) == 0:
    print("No data has been found")
    exit()

save_file(OUTPUT_PATH,"datapoints",data)

print("\ndone")
