import csv
import json
import pandas as pd
from pathlib import Path
# from neo4j import GraphDatabase
# from d4kms_generic import ServiceEnvironment
from d4kms_service import Neo4jConnection

print("\033[H\033[J") # Clears terminal window in vs code

def write_debug(data):
    OUTPUT_FILE = OUTPUT_PATH / 'debug-python.txt'
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


VS_DATA = Path.cwd() / "data" / "input" / "vs.json"
print("Reading",VS_DATA)
assert VS_DATA.exists(), "VS_DATA not found"
with open(VS_DATA) as f:
    vs_data = json.load(f)

OUTPUT_PATH = Path.cwd() / "data" / "output"
assert OUTPUT_PATH.exists(), "OUTPUT_PATH not found"

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
    "Pulse Rate": "Pulse Rate",
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

# unique_labels_visits = [tuple({"VSTEST":row['VSTEST'],"VISIT":row['VISIT']}) for row in vs_data]
unique_test_visit = set()
unique_labels_visits = []
for row in vs_data:
    test_visit = f"{clean(row['VSTEST'])}{clean(row['VISIT'])}"
    if test_visit not in unique_test_visit:
        unique_test_visit.add(test_visit)
        unique_labels_visits.append({"VSTEST":row['VSTEST'],"VISIT":row['VISIT']})
    # unique_labels_visits = [{"VSTEST":row['VSTEST'],"VISIT":row['VISIT']} for row in vs_data]

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
# rows = [row for row in vs_data]
# for row in rows:
# for row in vs_data:
for row in unique_labels_visits:
    # datapoint_root = f"{row['USUBJID']}/{row['DOMAIN']}/{clean(row['LBSEQ'])}"
    if row['VSTEST'] in DATA_LABELS_TO_BC_LABELS:
        bc_label = DATA_LABELS_TO_BC_LABELS[row['VSTEST']]
    else:
        bc_label = ""
        print("Add ",row['VSTEST'])
    # print("bc_label",bc_label)
    properties = get_bc_properties(db, bc_label,row)
    # print("properties",properties)
    if properties:
        good.append([bc_label])
    elif row['VISIT'] in DATA_VISITS_TO_ENCOUNTER_LABELS:
        visit = DATA_VISITS_TO_ENCOUNTER_LABELS[row['VISIT']]
        print("DataContract query did not yield any results",row['VISIT'],visit,bc_label)
        print("  Trying to fake visit",bc_label,visit)
        query = f"""
            MATCH (bc:BiomedicalConcept)-[:PROPERTIES_REL]->(bcp)<-[:PROPERTIES_REL]-(dc:DataContract)-[:INSTANCES_REL]->(act_inst)
            WHERE  bc.label = '{bc_label}'
            return bc.label as BC_LABEL, bcp.name as BCP_NAME, '{visit}' as ENTOUNTER_LABEL, dc.uri as DC_URI
        """
        query = f"""
            // Get Activity, ScheduledTimeline, Timing and ScheduledActivityInstance
            MATCH (actl:Activity)-[:TIMELINE_REL]->(stl:ScheduleTimeline {{entryCondition:'Automatic execution'}})
            MATCH (stl)-[:TIMINGS_REL]-(t:Timing)
            MATCH (t)-[:RELATIVE_TO_SCHEDULED_INSTANCE_REL]-(sai:ScheduledActivityInstance)
            return *
        """
        # print("query",query)
        queries.append("extra")
        queries.append(query)
        results = db.query(query)
        # queries.append(results)
        if results != None and results != []:
        #     properties = [result.data() for result in results]
            rs = [result.data() for result in results]
            for r in rs:
                # queries.append(r)
                for q in r.items():
                    queries.append(q)

        # print("---results",results)
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

write_debug(queries)
# write_debug(bad)

OUTPUT_FILE = OUTPUT_PATH / "data_contracts.json"
print("Saving to",OUTPUT_FILE)
with open(OUTPUT_FILE, 'w') as f:
    f.write(json.dumps(data, indent = 2))
