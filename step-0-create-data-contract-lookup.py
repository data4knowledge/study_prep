import csv
import json
import pandas as pd
from pathlib import Path
from d4kms_service import Neo4jConnection, ServiceEnvironment
import utility.debug

print("\033[H\033[J") # Clears terminal window in vs code

def clean(txt: str):
    txt = txt.replace(".","/")
    txt = txt.replace(" ","")
    return txt

def add_issue(*txts):
    issues.append(" ".join(txts))


def get_bc_properties(db, bc_label, row):
    if row['VISIT'] in DATA_VISITS_TO_ENCOUNTER_LABELS:
        visit = DATA_VISITS_TO_ENCOUNTER_LABELS[row['VISIT']]
    else:
        add_issue("visit not found:",row['VISIT'])
        return []
    query = f"""
    MATCH (bc:BiomedicalConcept)-[:PROPERTIES_REL]->(bcp)<-[:PROPERTIES_REL]-(dc:DataContract)-[:INSTANCES_REL]->(act_inst)-[:ENCOUNTER_REL]-(enc)
    WHERE enc.label = '{visit}'
    AND  bc.label = '{bc_label}'
    return bc.label as BC_LABEL, bcp.name as BCP_NAME, enc.label as ENCOUNTER_LABEL, dc.uri as DC_URI
    """
    results = db.query(query)
    if results == None:
        add_issue("DataContract has errors in it",row['VISIT'],visit,bc_label,query)
        return []
    return [result.data() for result in results]

def get_bc_properties_sub_timeline(db, bc_label, tpt, row):
    if row['VISIT'] in DATA_VISITS_TO_ENCOUNTER_LABELS:
        visit = DATA_VISITS_TO_ENCOUNTER_LABELS[row['VISIT']]
    else:
        add_issue("visit not found:",row['VISIT'])
        return []
    query = f"""
        match (msai:ScheduledActivityInstance)<-[:INSTANCES_REL]-(dc:DataContract)-[:INSTANCES_REL]-(ssai:ScheduledActivityInstance)
        match (msai)-[:ENCOUNTER_REL]->(enc:Encounter)
        match (ssai)<-[:RELATIVE_FROM_SCHEDULED_INSTANCE_REL]-(t:Timing)
        match (dc)-[:PROPERTIES_REL]->(bcp:BiomedicalConceptProperty)<-[:PROPERTIES_REL]-(bc:BiomedicalConcept)
        WHERE enc.label = '{visit}'
        and    t.value = '{tpt}'
        AND  bc.label = '{bc_label}'
        return bc.label as BC_LABEL, bcp.name as BCP_NAME, enc.label as ENCOUNTER_LABEL, t.value as TIMEPOINT_VALUE, dc.uri as DC_URI
    """
    results = db.query(query)
    if results == None:
        add_issue("timeline DataContract query has errors in it",visit,bc_label,tpt,query)
        return []
    return [result.data() for result in results]


# VS_DATA = Path.cwd() / "data" / "input" / "vs.json"
# print("Reading",VS_DATA)
# assert VS_DATA.exists(), "VS_DATA not found"
# with open(VS_DATA) as f:
#     vs_data = json.load(f)


LB_DATA = Path.cwd() / "data" / "input" / "lb.json"
print("Reading",LB_DATA)
assert LB_DATA.exists(), "LB_DATA not found"
with open(LB_DATA) as f:
    lb_data = json.load(f)

OUTPUT_PATH = Path.cwd() / "data" / "output"
assert OUTPUT_PATH.exists(), "OUTPUT_PATH not found"

DATA_LABELS_TO_BC_LABELS = {
    'Temperature': 'Body Temperature',
    'Weight': 'Body Weight',
    'Height': 'Body Height',
    'Alanine Aminotransferase': 'Alanine Aminotransferase Measurement',
    'Sodium': 'Sodium Measurement',
    'Aspartate Aminotransferase': 'Aspartate Aminotransferase Measurement',
    'Potassium': 'Potassium Measurement',
    'Albumin': 'Albumin Measurement',
    'Creatinine': 'Creatinine Measurement',
    'Alkaline Phosphatase': 'Alkaline Phosphatase Measurement',
    'Diastolic Blood Pressure': 'Diastolic Blood Pressure',
    'Systolic Blood Pressure': 'Systolic Blood Pressure',
    'Pulse Rate': 'Heart Rate',
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

unique_test_visit = set()
unique_labels_visits = []
# for row in vs_data:
#     # if 'VSTPT' in row:
#     if row['VSTPT'] != "":
#         test_visit = f"{clean(row['VSTEST'])}{clean(row['VISIT'])}{clean(row['VSTPT'])}"
#     else:
#         test_visit = f"{clean(row['VSTEST'])}{clean(row['VISIT'])}"

#     if test_visit not in unique_test_visit:
#         unique_test_visit.add(test_visit)
#         if row['VSTPT'] != "":
#             unique_labels_visits.append({"VSTEST":row['VSTEST'],"VISIT":row['VISIT'],"VSTPT":row['VSTPT']})
#         else:
#             unique_labels_visits.append({"VSTEST":row['VSTEST'],"VISIT":row['VISIT']})

for row in lb_data:
    if 'LBTPT' in row and row['LBTPT'] != "":
        test_visit = f"{clean(row['LBTEST'])}{clean(row['VISIT'])}{clean(row['LBTPT'])}"
    else:
        test_visit = f"{clean(row['LBTEST'])}{clean(row['VISIT'])}"

    if test_visit not in unique_test_visit:
        unique_test_visit.add(test_visit)
        if  'LBTPT' in row and row['LBTPT'] != "":
            unique_labels_visits.append({"LBTEST":row['LBTEST'],"VISIT":row['VISIT'],"LBTPT":row['LBTPT']})
        else:
            unique_labels_visits.append({"LBTEST":row['LBTEST'],"VISIT":row['VISIT']})

def check_connection():
    sv = ServiceEnvironment()
    self._db_name = sv.get('NEO4J_DB_NAME')
    self._url = sv.get('NEO4J_URI')
    self._usr = sv.get('NEO4J_USERNAME')
    self._pwd = sv.get('NEO4J_PASSWORD')


print("Connecting to Neo4j...",end="")
print("connected")
db = Neo4jConnection()

# Add LB data to the graph
# add_vs(db, vs_data, subjects)
print("Looping VS")
all_data_contracts = []
issues = []
matches = []
mismatches = []
debug = []

# for row in unique_labels_visits:
#     # datapoint_root = f"{row['USUBJID']}/{row['DOMAIN']}/{clean(row['LBSEQ'])}"
#     tpt = ""
#     if row['VSTEST'] in DATA_LABELS_TO_BC_LABELS:
#         bc_label = DATA_LABELS_TO_BC_LABELS[row['VSTEST']]
#     else:
#         bc_label = ""
#         add_issue("Add test",row['VSTEST'])
#     if 'VSTPT' in row and row['VSTPT'] != "":
#         tpt = DATA_TPT_TO_TIMING_LABELS[row['VSTPT']]
#         properties = get_bc_properties_sub_timeline(db, bc_label, tpt,row)
#     else:
#         properties = get_bc_properties(db, bc_label,row)
#     if properties:
#         matches.append([bc_label])
#     else:
#         mismatches.append([bc_label,row['VISIT'],tpt])
#     for property in properties:
#         if property in all_data_contracts:
#             True
#         else:
#             all_data_contracts.append(property)

for row in unique_labels_visits:
    # datapoint_root = f"{row['USUBJID']}/{row['DOMAIN']}/{clean(row['LBSEQ'])}"
    tpt = ""
    if row['LBTEST'] in DATA_LABELS_TO_BC_LABELS:
        bc_label = DATA_LABELS_TO_BC_LABELS[row['LBTEST']]
    else:
        bc_label = ""
        add_issue("Add test",row['LBTEST'])
    if 'LBTPT' in row and row['LBTPT'] != "":
        tpt = DATA_TPT_TO_TIMING_LABELS[row['LBTPT']]
        properties = get_bc_properties_sub_timeline(db, bc_label, tpt,row)
    else:
        properties = get_bc_properties(db, bc_label,row)
    if properties:
        matches.append([bc_label])
    else:
        mismatches.append([bc_label,row['VISIT'],tpt])
    for property in properties:
        if property in all_data_contracts:
            True
        else:
            all_data_contracts.append(property)

db.close()

data = all_data_contracts
print("Number of contracts and properties in VS;",len(data))
print("Number of matches   :",len(matches))
print("Number of mismatches:",len(mismatches), "(e.g. visit not defined in study)")
print("")

for issue in set([issues for issues in issues]):
    print(issue)
print("")


OUTPUT_FILE = OUTPUT_PATH / "data_contracts.json"
print("Saving to",OUTPUT_FILE)
with open(OUTPUT_FILE, 'w') as f:
    f.write(json.dumps(data, indent = 2))
print("done")
