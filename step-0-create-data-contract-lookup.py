import os
import csv
from pathlib import Path
# from neo4j import GraphDatabase
# from d4kms_generic import ServiceEnvironment
from d4kms_service import Neo4jConnection

print("\033[H\033[J") # Clears terminal window in vs code

def get_data(file):
    with open(file, mode ='r')as file:
        csv_file = csv.DictReader(file)
        data = list(csv_file)
    if len(list(data[0].keys())) < 3:
        print("I don't think the csv was read properly")
    return data


def clean(txt: str):
    txt = txt.replace(".","/")
    txt = txt.replace(" ","")
    return txt


def get_bc_properties(db, bc_label, row):
    if row['VISIT'] in DATA_VISITS_TO_ENCOUNTER_LABELS:
        visit = DATA_VISITS_TO_ENCOUNTER_LABELS[row['VISIT']]
    else:
        print("visit not found:",row['VISIT'])
        return []
    query = f"""
    MATCH (bc:BiomedicalConcept)-[:PROPERTIES_REL]->(bcp)<-[:PROPERTIES_REL]-(dc:DataContract)-[:INSTANCES_REL]->(act_inst)-[:ENCOUNTER_REL]-(enc)
    WHERE enc.label = '{visit}'
    AND  bc.label = '{bc_label}'
    return bc.label as BC_LABEL, bcp.name as BCP_NAME, enc.label as ENCOUNTER_LABEL, dc.uri as DC_URI
    """
    # print('bcp query',query)
    results = db.query(query)
    # print('results',results)
    if results == None:
        print("DataContract has errors in it",row['VISIT'],visit,bc_label,query)
        return []
    if results == []:
        print("DataContract query did not yield any results",row['VISIT'],visit,bc_label)
        return []
    # print("contract query alright")
    return [result.data() for result in results]


VS_DATA = Path.cwd() / "data" / "vs.csv"
assert VS_DATA.exists(), "VS_DATA not found"
vs_data = get_data(VS_DATA)

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
strs = set()
unique_labels_visits = []
for row in vs_data:
    x = f"{clean(row['VSTEST'])}{clean(row['VISIT'])}"
    if x not in strs:
        strs.add(x)
        unique_labels_visits.append({"VSTEST":row['VSTEST'],"VISIT":row['VISIT']})
    # unique_labels_visits = [{"VSTEST":row['VSTEST'],"VISIT":row['VISIT']} for row in vs_data]

db = Neo4jConnection()

# Add vs data to the graph
# add_vs(db, vs_data, subjects)
print("Looping VS")
all_data = []
# rows = [row for row in vs_data]
# for row in rows:
# for row in vs_data:
for row in unique_labels_visits:
    # datapoint_root = f"{row['USUBJID']}/{row['DOMAIN']}/{clean(row['LBSEQ'])}"
    bc_label = DATA_LABELS_TO_BC_LABELS[row['VSTEST']]
    # print("bc_label",bc_label)
    properties = get_bc_properties(db, bc_label,row)
    for property in properties:
        if property in all_data:
            True
        else:
            all_data.append(property)

db.close()


# print("data[0].__class__",data[0].__class__)
# print("data[0].keys()",data[0].keys())
# data = set(all_data)
data = all_data
output_variables = list(data[0].keys())

OUTPUT_FILE = OUTPUT_PATH / "data_contracts.csv"
print("Saving to",OUTPUT_FILE)
with open(OUTPUT_FILE, 'w') as csv_file:
    # creating a csv dict writer object
    writer = csv.DictWriter(csv_file, fieldnames=output_variables)

    # writing headers (field names)
    writer.writeheader()

    # writing data rows
    writer.writerows(data)
print("Done")
