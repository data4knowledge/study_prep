import json
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

# def clear_created_nodes(db):
#     query = "match (n:Subject) detach delete n"
#     results = db.query(query)
#     query = "match (n:Datapoint) detach delete n"
#     results = db.query(query)


DATA_LABELS_TO_BC_LABELS = {
    "Temperature": "Body Temperature",
    "Weight": "Body Weight",
    "Height": "Body Height",
    "Alanine Aminotransferase": "Alanine Aminotransferase Measurement",
    "Sodium": "Sodium Measurement",
    "Aspartate Aminotransferase": "Aspartate Aminotransferase Measurement",
    "Potassium": "Potassium Measurement",
    "Albumin": "Albumin Measurement",
    "Creatinine": "Creatinine Measurement",
    "Alkaline Phosphatase": "Alkaline Phosphatase Measurement",
    "Diastolic Blood Pressure": "Diastolic Blood Pressure",
    "Systolic Blood Pressure": "Systolic Blood Pressure",
    "ALP": "",
    "ALT": "",
    "K": "",
    "ALB": "",
    "SODIUM": "",
    "AST": "",
    "CREAT": ""
}
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
# Unknown visits
# 'RETRIEVAL': 'CHECK', 
# 'AMBUL ECG PLACEMENT': 'CHECK', 
# 'AMBUL ECG REMOVAL': 'CHECK'

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

TEST_ROW_VARIABLE_TO_BC_PROPERTY = {
    "Weight": {
        "VSORRES": "Clinical Test Result",
        "VSORRESU": "Unit of Weight",
    },
    "Height": {
        "VSORRES": "Clinical Test Result",
        "VSORRESU": "Unit of Height",
    },
    "Temperature": {
        "VSORRES": "Clinical Test Result",
        "VSORRESU": "Unit of Temperature",
    },
    "Diastolic Blood Pressure": {
        "VSORRES": "Clinical Test Result",
        "VSORRESU": "Unit of Pressure",
    },
    "Systolic Blood Pressure": {
        "VSORRES": "Clinical Test Result",
        "VSORRESU": "Unit of Pressure",
    },
}


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
            encounter = f"E:{row['VISIT']}"
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

# def get_data_contract(row,property):
#     # dc_uri = data_contracts.find
#     if row['VISIT'] in DATA_VISITS_TO_ENCOUNTER_LABELS:    
#         visit = DATA_VISITS_TO_ENCOUNTER_LABELS[row['VISIT']]
#         bc_label = DATA_LABELS_TO_BC_LABELS[row['VSTEST']]
#         # print("--find",bc_label,property,visit)
#         dc_item = next((item for item in data_contracts if item["BC_LABEL"] == bc_label and item['BCP_NAME'] == property and item['ENCOUNTER_LABEL'] == visit), None)
#         if dc_item != None:
#             if "DC_URI" in dc_item:
#                 return dc_item['DC_URI']
#             else:
#                 print("Missing DC_URI", visit, bc_label)
#         else:
#             return None
#     return None

def get_data_contract(encounter,bc_label,property):
    # print("--find",bc_label,property,encounter)
    dc_item = next((item for item in data_contracts if item["BC_LABEL"] == bc_label and item['BCP_NAME'] == property and item['ENCOUNTER_LABEL'] == encounter), None)
    if dc_item != None:
        ok.append(dc_item)
        if "DC_URI" in dc_item:
            return dc_item['DC_URI']
        else:
            print("Missing DC_URI", encounter, bc_label)
    else:
        # dc_item = next((item for item in data_contracts if item["BC_LABEL"] == bc_label), None)
        # if dc_item == None:
        #     issues.append(f"Miss BC_LABEL {bc_label}")
        # dc_item = next((item for item in data_contracts if item['BCP_NAME'] == property), None)
        # issues.append(f"Miss BCP_NAME {dc_item['BCP_NAME']}")
        # if dc_item == None:
        #     issues.append(f"Miss BCP_NAME {property}")
        # dc_item = next((item for item in data_contracts if item['ENCOUNTER_LABEL'] == encounter), None)
        # if dc_item == None:
        #     issues.append(f"Miss ENCOUNTER_LABEL {encounter}")
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

# tests = [row['VSTESTCD'] for row in vs_data]
# print(len(tests))
# print(len(set(tests)))
# print(set(tests))

# {'DIABP', 'TEMP', 'HEIGHT', 'SYSBP', 'PULSE', 'WEIGHT'}
# vs_data = [row for row in vs_data if row['VSTESTCD'] in ['TEMP', 'HEIGHT', 'WEIGHT']]
vs_data = [row for row in vs_data if row['VSTESTCD'] in ['DIABP']]


issues = []
ok = []
print("Creating datapoint and value")
data = []
# output_variables = ['DATAPOINT','VALUE','ENCOUNTER']
# for row in vs_data[0:5]:
for row in vs_data:
    # print(list(row.keys()))
    datapoint_root = f"{row['USUBJID']}/{row['DOMAIN']}/{clean(row['VSSEQ'])}"
    item = {}

    # Result
    encounter = get_encounter(row)
    bc_label = get_bc_label(row['VSTEST'])
    property = get_property_for_variable(row['VSTEST'],'VSORRESU')
    data_contract = get_data_contract(encounter,bc_label,property)

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
        issues.append(f"No dc result bc_label: {bc_label} property: {property} encounter: {encounter}")

    # Unit
    encounter = get_encounter(row)
    bc_label = get_bc_label(row['VSTEST'])
    property = get_property_for_variable(row['VSTEST'],'VSORRESU')
    data_contract = get_data_contract(encounter,bc_label,property)
    if data_contract:
        item = {}
        item['USUBJID'] = row['USUBJID']
        item['DC_URI'] = data_contract
        item['DATAPOINT'] = f"{datapoint_root}{row['VSTESTCD']}/unit"
        item['VALUE'] = f"{row['VSORRESU']}"
        # item['ENCOUNTER'] = encounter
        data.append(item)
    else:
        issues.append(f"No dc unit {row['VSTESTCD']} {row['VISIT']} encounter: {encounter} property: {property}")


print("---OK:",len(ok))
# for o in ok:
#     print(o)
print("---issues",len(issues))
for issue in issues:
    print(issue)

output_variables = list(data[0].keys())
OUTPUT_FILE = OUTPUT_PATH / "datapoints.csv"
print("Saving to",OUTPUT_FILE)
with open(OUTPUT_FILE, 'w') as csv_file:
    writer = csv.DictWriter(csv_file, fieldnames=output_variables)
    writer.writeheader()
    writer.writerows(data)
print("Done")
