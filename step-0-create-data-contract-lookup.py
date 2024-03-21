import csv
import json
import pandas as pd
from pathlib import Path
# from neo4j import GraphDatabase
# from d4kms_generic import ServiceEnvironment
from d4kms_service import Neo4jConnection

print("\033[H\033[J") # Clears terminal window in vs code

def write_debug(data):
    TMP_PATH = Path.cwd() / "tmp"
    OUTPUT_FILE = TMP_PATH / 'debug-python.txt'
    print("Writing debug...",OUTPUT_FILE, end="")
    with open(OUTPUT_FILE, 'w') as f:
        for it in data:
            f.write(str(it))
            f.write('\n')
    print(" ...done")

def clean(txt: str):
    txt = txt.replace(".","/")
    txt = txt.replace(" ","")
    return txt


def get_bc_properties(db, bc_label, row):
    if row['VISIT'] in DATA_VISITS_TO_ENCOUNTER_LABELS:
        visit = DATA_VISITS_TO_ENCOUNTER_LABELS[row['VISIT']]
    else:
        # print("visit not found:",row['VISIT'])
        issues.append("visit not found:"+row['VISIT'])
        return []
    query = f"""
    MATCH (bc:BiomedicalConcept)-[:PROPERTIES_REL]->(bcp)<-[:PROPERTIES_REL]-(dc:DataContract)-[:INSTANCES_REL]->(act_inst)-[:ENCOUNTER_REL]-(enc)
    WHERE enc.label = '{visit}'
    AND  bc.label = '{bc_label}'
    return bc.label as BC_LABEL, bcp.name as BCP_NAME, enc.label as ENCOUNTER_LABEL, dc.uri as DC_URI
    """
    queries.append("standard")
    queries.append(query)
    results = db.query(query)
    # print('results',results)
    if results == None:
        print("DataContract has errors in it",row['VISIT'],visit,bc_label)
        # print("Query",query)
        return []
    if results == []:
        # print("query",query)
        return []
    # print("contract query alright")
    return [result.data() for result in results]

def get_bc_properties_sub_timeline(db, bc_label, tpt, row):
    if row['VISIT'] in DATA_VISITS_TO_ENCOUNTER_LABELS:
        visit = DATA_VISITS_TO_ENCOUNTER_LABELS[row['VISIT']]
    else:
        # print("visit not found:",row['VISIT'])
        issues.append("visit not found:"+row['VISIT'])
        return []
    query = f"""
        match (msai:ScheduledActivityInstance)<-[:DC_TO_MAIN_TIMELINE]-(dc:DataContract)-[:INSTANCES_REL]-(ssai:ScheduledActivityInstance)
        match (msai)-[:ENCOUNTER_REL]->(enc:Encounter)
        match (ssai)<-[:RELATIVE_FROM_SCHEDULED_INSTANCE_REL]-(t:Timing)
        match (dc)-[:PROPERTIES_REL]->(bcp:BiomedicalConceptProperty)<-[:PROPERTIES_REL]-(bc:BiomedicalConcept)
        WHERE enc.label = '{visit}'
        and    t.value = '{tpt}'
        AND  bc.label = '{bc_label}'
        return bc.label as BC_LABEL, bcp.name as BCP_NAME, enc.label as ENCOUNTER_LABEL, t.value as TIMEPOINT_VALUE, dc.uri as DC_URI
    """
    # print("query",query)
    queries.append("extra")
    queries.append(query)
    results = db.query(query)
    if results == None:
        print("timeline DataContract query has errors in it",visit,bc_label,tpt)
        # print("Query",query)
        queries.append(f"timeline DataContract query has errors in it {visit} {bc_label} {tpt}")
        queries.append(query)
        return []
    if results == []:
        # print("query",query)
        return []
    return [result.data() for result in results]


VS_DATA = Path.cwd() / "data" / "input" / "vs.json"
print("Reading",VS_DATA)
assert VS_DATA.exists(), "VS_DATA not found"
with open(VS_DATA) as f:
    vs_data = json.load(f)

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

# unique_labels_visits = [tuple({"VSTEST":row['VSTEST'],"VISIT":row['VISIT']}) for row in vs_data]
unique_test_visit = set()
unique_labels_visits = []
for row in vs_data:
    # if 'VSTPT' in row:
    if row['VSTPT'] != "":
        test_visit = f"{clean(row['VSTEST'])}{clean(row['VISIT'])}{clean(row['VSTPT'])}"
    else:
        test_visit = f"{clean(row['VSTEST'])}{clean(row['VISIT'])}"

    if test_visit not in unique_test_visit:
        unique_test_visit.add(test_visit)
        if row['VSTPT'] != "":
            unique_labels_visits.append({"VSTEST":row['VSTEST'],"VISIT":row['VISIT'],"VSTPT":row['VSTPT']})
        else:
            unique_labels_visits.append({"VSTEST":row['VSTEST'],"VISIT":row['VISIT']})

# write_debug(unique_labels_visits)

print("Connecting to Neo4j...",end="")
db = Neo4jConnection()
print("connected")

# Add vs data to the graph
# add_vs(db, vs_data, subjects)
print("Looping VS")
all_data = []
issues = []
good = []
queries = []
bad = []

# for row in unique_labels_visits[0:5]:
for row in unique_labels_visits:
    # datapoint_root = f"{row['USUBJID']}/{row['DOMAIN']}/{clean(row['LBSEQ'])}"
    tpt = ""
    if row['VSTEST'] in DATA_LABELS_TO_BC_LABELS:
        bc_label = DATA_LABELS_TO_BC_LABELS[row['VSTEST']]
    else:
        bc_label = ""
        print("Add ",row['VSTEST'])
    # print("bc_label",bc_label)
    if 'VSTPT' in row and row['VSTPT'] != "":
        tpt = DATA_TPT_TO_TIMING_LABELS[row['VSTPT']]
        properties = get_bc_properties_sub_timeline(db, bc_label, tpt,row)
    else:
        properties = get_bc_properties(db, bc_label,row)
    # print("properties",properties)
    if properties:
        good.append([bc_label])
    else:
        bad.append([bc_label,row['VISIT']])
    for property in properties:
        if property in all_data:
            True
        else:
            all_data.append(property)

db.close()

data = all_data
print("len(data)",len(data))
print("len(good)",len(good))
print("len(bad)",len(bad))
output_variables = list(data[0].keys())

# write_debug(queries)
write_debug(bad)

OUTPUT_FILE = OUTPUT_PATH / "data_contracts.json"
print("Saving to",OUTPUT_FILE)
with open(OUTPUT_FILE, 'w') as f:
    f.write(json.dumps(data, indent = 2))
