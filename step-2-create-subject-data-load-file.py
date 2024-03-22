import json
import csv
from pathlib import Path

print("\033[H\033[J") # Clears terminal window in vs code

def write_debug(data):
    TMP_PATH = Path.cwd() / "tmp"
    OUTPUT_FILE = TMP_PATH / 'debug-python.txt'
    with open(OUTPUT_FILE, 'w') as f:
        for it in data:
            f.write(str(it))
            f.write('\n')

def get_data(file):
    with open(file, mode ='r')as file:
        csv_file = csv.DictReader(file)
        data = list(csv_file)
    if len(list(data[0].keys())) < 3:
        print("I don't think the csv was read properly")
    return data

# def clear_created_nodes(db):
#     query = "match (n:Subject) detach delete n"
#     results = db.query(query)
#     query = "match (n:Datapoint) detach delete n"
#     results = db.query(query)


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

result_name = 'Clinical Test Result'
# result_name = 'Vital Signs Result'
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


def clean(txt):
    result = ""
    if isinstance(txt, str):
        result = txt
    elif isinstance(txt, float):
        # issues.append(f"float->str {txt}->{str(txt)}")
        result = str(txt)
    else:
        print("clean does not now of:",txt.__class__,txt)

    return result.replace(".","/")

# def mint_datapoint(stuff):
#     datapoint_uri = f"{stuff}"
#     return datapoint_uri

# def add_datapoint():
#     datapoint = {}
#     return datapoint

# "Clinical Test Result":["VSORRES"],
# "Unit of Temperature":["VSORRESU"],
# "Anatomic Site":["VSLOC"],
# "Unit of Weight":["VSORRESU"],
# "Unit of Height":["VSORRESU"],
# "Laboratory Test Result":["LBORRES"],
# "Laboratory Test Fasting Status":["LBFAST"],
# "Unit of Catalytic Activity Concentration":[""],
# "Biospecimen Type":["LBSPEC"],
# "Unit of Concentration":["LBORRES"],
# "Molarity Unit":["LBORRES"],
# "Reported Event Term":["AETERM"],
# "Dictionary-derived Term":["AEDECOD"],
# "Category":["VSCAT","LBCAT"],
# "Subcategory":[""],
# "Observation Start Date Time":["VSDTC","LBDTC"],
# "Unit of Pressure":[""],
# "Laterality":[""],
# "Test Method":[""],
# "Body Position":[""],
# "Count per Minute":[""]


def get_property_for_variable(test,variable):
    # property = next((property for property,variables in VARIABLE_TO_PROPERTY.items() if variable in variables), None)
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
            # encounter = f"E:{row['VISIT']}"
    else:
        encounter = f"VISIT not in row"
    return encounter

def get_bc_label(thing):
    bc_label = ""
    if thing in DATA_LABELS_TO_BC_LABELS: 
        bc_label = DATA_LABELS_TO_BC_LABELS[thing]
    else:
        print("Add bc_label:",thing)
    return bc_label

def get_data_contract(encounter,bc_label,property,tpt):
    # print("--find",bc_label,property,encounter)
    if tpt == "":
        dc_item = next((item for item in data_contracts if item["BC_LABEL"] == bc_label and item['BCP_NAME'] == property and item['ENCOUNTER_LABEL'] == encounter), None)
    else:
        dc_item = next((item for item in data_contracts if item["BC_LABEL"] == bc_label and item['BCP_NAME'] == property and item['ENCOUNTER_LABEL'] == encounter and item['TIMEPOINT_VALUE'] == tpt), None)

    if dc_item != None:
        ok.append(dc_item)
        if "DC_URI" in dc_item:
            return dc_item['DC_URI']
        else:
            print("Missing DC_URI", encounter, bc_label)
    else:
        # issues.append(f"Miss ENCOUNTER_LABEL {encounter}")
        return None

DM_DATA = Path.cwd() / "data" / "output" / "enrolment.json"
assert DM_DATA.exists(), "DM_DATA not found"
print("Getting subjects from file",DM_DATA)
with open(DM_DATA) as f:
    dm_data = json.load(f)


DATA_CONTRACTS_LOOKUP = Path.cwd() / "data" / "output" / "data_contracts.json"
assert DATA_CONTRACTS_LOOKUP.exists(), "DATA_CONTRACTS_LOOKUP not found"
print("Getting data contracts from file",DATA_CONTRACTS_LOOKUP)
with open(DATA_CONTRACTS_LOOKUP) as f:
    data_contracts = json.load(f)

OUTPUT_PATH = Path.cwd() / "data" / "output"
assert OUTPUT_PATH.exists(), "OUTPUT_PATH not found"

# Get subjects from the enrolment file
print("Getting subjects from enrolment file")
subjects = [row['USUBJID'] for row in dm_data]


print("Getting VS data")
VS_DATA = Path.cwd() / "data" / "input" / "vs.json"
assert VS_DATA.exists(), "VS_DATA not found"
with open(VS_DATA) as f:
    vs_data = json.load(f)

# Subsetting VS
# tests = [row['VSTESTCD'] for row in vs_data]
# print(len(tests))
# print(len(set(tests)))
# print(set(tests))
# exit()
# {'DIABP', 'TEMP', 'HEIGHT', 'SYSBP', 'PULSE', 'WEIGHT'}
# vs_data = [row for row in vs_data if row['VSTESTCD'] in ['TEMP', 'HEIGHT', 'WEIGHT']]
# vs_data = [row for row in vs_data if row['VSTESTCD'] in ['DIABP']]
# vs_data = [row for row in vs_data[0:20]]


issues = []
ok = []
print("Creating datapoint and value")
data = []

# for row in vs_data[0:5]:
for row in vs_data:
    # print(list(row.keys()))
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
        # if row['VSTPT'] != "":

        data_contract = get_data_contract(encounter,bc_label,property,tpt)

        if data_contract:
            item['USUBJID'] = row['USUBJID']
            item['DC_URI'] = data_contract
            item['DATAPOINT'] = f"{datapoint_root}{row['VSTESTCD']}/result"
            item['VALUE'] = f"{row['VSORRES']}"
            # item['ENCOUNTER'] = encounter
            data.append(item)
        else:
            # issues.append(f"No data contract result {row['VSTESTCD']} {row['VISIT']} encounter: {encounter} property: {property}")
            # issues.append(f"No dc result {row['VSTESTCD']} {row['VISIT']} bc_label: {bc_label} property: {property} encounter: {encounter}")
            issues.append(f"No dc RESULT bc_label: {bc_label} - property: {property} - encounter: {encounter}")

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
            # item['ENCOUNTER'] = encounter
            data.append(item)
        else:
            issues.append(f"No dc UNIT bc_label: {bc_label} - encounter: {encounter} property: {property}")
    else:
            issues.append(f"Ignoring visit {row['VISIT']} encounter: {encounter}")
        

print("---Datapoint - Data contract matches:",len(ok))
print("---Non matching Datapoints (e.g. visit not defined)",len(issues))
for issue in set(issues):
    print(issue)

if len(data) == 0:
    print("No data has been found")
    exit()

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

print("Writing debug...", end="")
write_debug(issues)
print(" ...done")
save_file(OUTPUT_PATH,"datapoints",data)

print("Done")
