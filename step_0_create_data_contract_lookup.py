import os
import csv
import json
import pandas as pd
from pathlib import Path
from neo4j import GraphDatabase
from d4kms_service import Neo4jConnection, ServiceEnvironment
from operator import itemgetter
from utility.neo_utils import db_is_down
from utility.mappings import DATA_LABELS_TO_BC_LABELS, DATA_VISITS_TO_ENCOUNTER_LABELS, DATA_TPT_TO_TIMING_LABELS

OUTPUT_PATH = Path.cwd() / "data" / "output"
unique_data_contracts = []
issues = []
matches = []
mismatches = []
debug = []

def write_debug(name, data):
    from pathlib import Path
    import os
    TMP_PATH = Path.cwd() / "tmp" / "saved_debug"
    if not os.path.isdir(TMP_PATH):
      os.makedirs(TMP_PATH)
    OUTPUT_FILE = TMP_PATH / name
    print("Writing file...",OUTPUT_FILE.name,OUTPUT_FILE, end="")
    with open(OUTPUT_FILE, 'w') as f:
        for it in data:
            f.write(str(it))
            f.write('\n')
    print(" ...done")

def output_csv(name, data):
    OUTPUT_PATH = Path.cwd() / "data" / "output"
    OUTPUT_FILE = OUTPUT_PATH / name
    if OUTPUT_FILE.exists():
        os.unlink(OUTPUT_FILE)
    print("Saving to",OUTPUT_FILE)
    # output_variables = list(data[0].keys())
    output_variables = ['BC_LABEL','BCP_NAME','BCP_LABEL','ENCOUNTER_LABEL','TIMEPOINT_VALUE','DC_URI']
    with open(OUTPUT_FILE, 'w') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=output_variables)
        writer.writeheader()
        writer.writerows(data)

def add_debug(*txts):
    line = ""
    for txt in txts:
        line += str(txt)
    debug.append(line)

def clean(txt: str):
    txt = txt.replace(".","/")
    txt = txt.replace(" ","")
    return txt

def add_issue(*txts):
    issues.append(" ".join(txts))

def db_query(query):
    db = Neo4jConnection()
    result = []
    with db.session() as session:
        response = session.run(query)
        if response == None:
            print('query did not work"',query)
            exit()
        result = [x.data() for x in response]
    db.close()
    return result


def list_dc_bcp_name():
    OUTPUT_FILE = OUTPUT_PATH / "data_contracts.json"
    with open(OUTPUT_FILE) as f:
        data_contracts = json.load(f)

    results = []
    add_debug("=== Check DC_URIs")
    for dc in data_contracts:
        # add_debug(dc['DC_URI'])
        query = f"""
            match (dc:DataContract)-[:PROPERTIES_REL]->(bcp:BiomedicalConceptProperty)<-[:PROPERTIES_REL]-(bc:BiomedicalConcept)
            WHERE dc.uri = '{dc['DC_URI']}'
            return bcp.name as bcp_name, bc.name as bc_name
        """
        bcp_names = db_query(query)            
        if bcp_names:
            bcp_name = bcp_names[0]['bcp_name']
            bc_name = bcp_names[0]['bc_name']
            results.append(f"{bc_name} {bcp_name} : {dc['DC_URI']}")
        else:
            results.append(f"MISS {dc['DC_URI']}")
            print("!!! Data Conctract not found:", dc['DC_URI'])
    return results

def check_that_data_contracts_exist():
    OUTPUT_FILE = OUTPUT_PATH / "data_contracts.json"
    with open(OUTPUT_FILE) as f:
        data_contracts = json.load(f)

    missing_dcs = 0
    add_debug("=== Check DC_URIs")
    for dc in data_contracts:
        # add_debug(dc['DC_URI'])
        query = f"""
            match (dc:DataContract) WHERE dc.uri = '{dc['DC_URI']}'
            return dc.uri as uri
        """
        results = db_query(query)
        if results == []:
            add_debug("DC not found:",dc['DC_URI'])
            missing_dcs += 1
        else:
            pass
    return missing_dcs


def get_bc_properties( bc_label, row):
    results = []
    if row['VISIT'] in DATA_VISITS_TO_ENCOUNTER_LABELS:
        visit = DATA_VISITS_TO_ENCOUNTER_LABELS[row['VISIT']]
    else:
        add_issue("visit not found:",row['VISIT'])
        return []
    query = f"""
        MATCH (bc:BiomedicalConcept)-[:PROPERTIES_REL]->(bcp)<-[:PROPERTIES_REL]-(dc:DataContract)-[:INSTANCES_REL]->(act_inst)-[:ENCOUNTER_REL]-(enc)
        WHERE enc.label = '{visit}'
        AND  bc.label = '{bc_label}'
        return bc.label as BC_LABEL, bcp.name as BCP_NAME, bcp.label as BCP_LABEL, enc.label as ENCOUNTER_LABEL, dc.uri as DC_URI
    """
    results = db_query(query)
    if results == []:
        add_issue("DataContract has errors in it",row['VISIT'],visit,bc_label,query)
        return []
    return results

def get_bc_properties_dm( bc_label, dm_visit):
    if dm_visit in DATA_VISITS_TO_ENCOUNTER_LABELS:
        visit = DATA_VISITS_TO_ENCOUNTER_LABELS[dm_visit]
    else:
        add_issue("visit not found:",dm_visit)
        return []
    query = f"""
        MATCH (bc:BiomedicalConcept)-[:PROPERTIES_REL]->(bcp)<-[:PROPERTIES_REL]-(dc:DataContract)-[:INSTANCES_REL]->(act_inst)-[:ENCOUNTER_REL]-(enc)
        WHERE enc.label = '{visit}'
        AND  bc.label = '{bc_label}'
        return bc.label as BC_LABEL, bcp.name as BCP_NAME, bcp.label as BCP_LABEL, enc.label as ENCOUNTER_LABEL, dc.uri as DC_URI
    """
    results = db_query(query)
    if results == []:
        add_issue("DataContract has errors in it",visit,bc_label,query)
        return []
    return results

def get_bc_properties_sub_timeline(bc_label, tpt, row):
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
        return bc.label as BC_LABEL, bcp.name as BCP_NAME, bcp.label as BCP_LABEL, enc.label as ENCOUNTER_LABEL, t.value as TIMEPOINT_VALUE, dc.uri as DC_URI
    """
    results = db_query(query)
    if results == []:
        add_issue("timeline DataContract query has errors in it",visit,bc_label,tpt,query)
        return []
    else:
        return results

def get_bc_properties_ae(bc_label):
    # query = f"""
    #     MATCH (bc:BiomedicalConcept)-[:PROPERTIES_REL]->(bcp)<-[:PROPERTIES_REL]-(dc:DataContract)-[:INSTANCES_REL]->(act_inst)
    #     WHERE bc.label = '{bc_label}'
    #     return bc.label as BC_LABEL, bcp.name as BCP_NAME, bcp.label as BCP_LABEL, dc.uri as DC_URI
    # """
    query = f"""
        match (msai:ScheduledActivityInstance)<-[:INSTANCES_REL]-(dc:DataContract)-[:INSTANCES_REL]-(ssai:ScheduledActivityInstance)
        match (ssai)<-[:RELATIVE_FROM_SCHEDULED_INSTANCE_REL]-(t:Timing)
        match (dc)-[:PROPERTIES_REL]->(bcp:BiomedicalConceptProperty)<-[:PROPERTIES_REL]-(bc:BiomedicalConcept)
        WHERE bc.label = '{bc_label}'
        return bc.label as BC_LABEL, bcp.name as BCP_NAME, bcp.label as BCP_LABEL, dc.uri as DC_URI,"" as ENCOUNTER_LABEL
    """
    # print("ae query", query)
    results = db_query(query)
    if results == []:
        add_issue("DataContract has errors in it",bc_label,query)
        return []
    return results


def create_data_contracts_lookup():
    global unique_data_contracts
    assert OUTPUT_PATH.exists(), "OUTPUT_PATH not found"

    VS_DATA = Path.cwd() / "data" / "input" / "vs.json"
    add_debug("Reading",VS_DATA)
    assert VS_DATA.exists(), "VS_DATA not found"
    with open(VS_DATA) as f:
        vs_data = json.load(f)


    LB_DATA = Path.cwd() / "data" / "input" / "lb.json"
    add_debug("Reading",LB_DATA)
    assert LB_DATA.exists(), "LB_DATA not found"
    with open(LB_DATA) as f:
        lb_data = json.load(f)

    EX_DATA = Path.cwd() / "data" / "input" / "ex.json"
    add_debug("Reading",EX_DATA)
    assert EX_DATA.exists(), "EX_DATA not found"
    with open(EX_DATA) as f:
        ex_data = json.load(f)


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
                unique_labels_visits.append({"TEST":row['VSTEST'],"VISIT":row['VISIT'],"TPT":row['VSTPT']})
            else:
                unique_labels_visits.append({"TEST":row['VSTEST'],"VISIT":row['VISIT']})

    for row in lb_data:
        if 'LBTPT' in row and row['LBTPT'] != "":
            test_visit = f"{clean(row['LBTEST'])}{clean(row['VISIT'])}{clean(row['LBTPT'])}"
        else:
            test_visit = f"{clean(row['LBTEST'])}{clean(row['VISIT'])}"

        if test_visit not in unique_test_visit:
            unique_test_visit.add(test_visit)
            if  'LBTPT' in row and row['LBTPT'] != "":
                unique_labels_visits.append({"TEST":row['LBTEST'],"VISIT":row['VISIT'],"TPT":row['LBTPT']})
            else:
                unique_labels_visits.append({"TEST":row['LBTEST'],"VISIT":row['VISIT']})

    for row in ex_data:
        test_visit = f"{clean(row['EXTRT'])}{clean(row['VISIT'])}"
        if test_visit not in unique_test_visit:
            unique_test_visit.add(test_visit)
            unique_labels_visits.append({"TEST":row['EXTRT'],"VISIT":row['VISIT']})

    print("Connecting to Neo4j...",end="")
    if db_is_down():
        print("is not running")
        exit()
    print("connected")

    for row in unique_labels_visits:
        tpt = ""
        if row['TEST'] in DATA_LABELS_TO_BC_LABELS:
            bc_label = DATA_LABELS_TO_BC_LABELS[row['TEST']]
        else:
            bc_label = ""
            add_issue("Add test",row['TEST'])
        if 'TPT' in row and row['TPT'] != "":
            tpt = DATA_TPT_TO_TIMING_LABELS[row['TPT']]
            properties = get_bc_properties_sub_timeline(bc_label, tpt,row)
        else:
            properties = get_bc_properties(bc_label,row)
        if properties:
            matches.append([bc_label,row['VISIT'],tpt,[x['BCP_NAME'] for x in properties]])
        else:
            mismatches.append([bc_label,row['VISIT'],tpt])
        for property in properties:
            if property in unique_data_contracts:
                True
            else:
                unique_data_contracts.append(property)

    # Add DM stuff
    dm_visit ='SCREENING 1'
    bc_label = "Sex"
    properties = get_bc_properties_dm(bc_label,dm_visit)
    if properties:
        matches.append([bc_label,[x['BCP_NAME'] for x in properties]])
    else:
        mismatches.append([bc_label,dm_visit])
    for property in properties:
        if property in unique_data_contracts:
            True
        else:
            unique_data_contracts.append(property)
    bc_label = "Race"
    properties = get_bc_properties_dm(bc_label,dm_visit)
    if properties:
        matches.append([bc_label,[x['BCP_NAME'] for x in properties]])
    else:
        mismatches.append([bc_label,dm_visit])
    for property in properties:
        if property in unique_data_contracts:
            True
        else:
            unique_data_contracts.append(property)
    bc_label = "Informed Consent Obtained"
    properties = get_bc_properties_dm(bc_label,dm_visit)
    if properties:
        matches.append([bc_label,[x['BCP_NAME'] for x in properties]])
    else:
        mismatches.append([bc_label,dm_visit])
    for property in properties:
        if property in unique_data_contracts:
            True
        else:
            unique_data_contracts.append(property)
    bc_label = "Date of Birth"
    properties = get_bc_properties_dm(bc_label,dm_visit)
    if properties:
        matches.append([bc_label,[x['BCP_NAME'] for x in properties]])
    else:
        mismatches.append([bc_label,dm_visit])
    for property in properties:
        if property in unique_data_contracts:
            add_debug("DoB not adding",property)
            True
        else:
            add_debug("DoB adding",property)
            unique_data_contracts.append(property)

    # Add AE stuff
    properties = get_bc_properties_ae('Adverse Event Prespecified')
    if properties:
        matches.append([bc_label,[x['BCP_NAME'] for x in properties]])
    else:
        mismatches.append([bc_label])
    for property in properties:
        if property in unique_data_contracts:
            True
        else:
            unique_data_contracts.append(property)


    print("Number of contracts and properties:",len(unique_data_contracts))
    print("Number of matches with data:",len(matches))
    print("Number of mismatches       :",len(mismatches), "(e.g. visit not defined in study)")
    print("")

    for issue in set([issues for issues in issues]):
        print(issue)
    add_debug("== Issues")
    [add_debug(x) for x in issues]
    add_debug("\n== mismaches")
    [add_debug(x) for x in mismatches]
    add_debug("\n== matches")
    [add_debug(x) for x in matches]

    # Sort before saving, so it is easier to spot differences
    # unique_data_contracts = sorted(unique_data_contracts, key=itemgetter('BC_LABEL','BCP_NAME','BCP_LABEL','ENCOUNTER_LABEL'))
    def sortk(item):
        if 'ENCOUNTER' in item:
            return (item['BC_LABEL'],item['BCP_NAME'],item['BCP_LABEL'],item['ENCOUNTER_LABEL'])
        else:
            return (item['BC_LABEL'],item['BCP_NAME'],item['BCP_LABEL'])
    unique_data_contracts = sorted(unique_data_contracts, key=sortk)
    # unique_data_contracts = sorted(unique_data_contracts, key= lambda k: 'ENCOUNTER' not in k ('BC_LABEL','BCP_NAME','BCP_LABEL','ENCOUNTER_LABEL'))

    OUTPUT_FILE = OUTPUT_PATH / "data_contracts.json"
    print("Deleting old file ",OUTPUT_FILE, end="")
    if OUTPUT_FILE.exists():
        Path.unlink(OUTPUT_FILE)
    print(" done")
    print("Saving to ",OUTPUT_FILE, end="")
    with open(OUTPUT_FILE, 'w') as f:
        f.write(json.dumps(unique_data_contracts, indent = 2))
    print(" done")

    output_csv("data_contracts.csv",unique_data_contracts)

    count = check_that_data_contracts_exist()
    if count:
        print("\n-- THERE ARE MISSING DATA_CONTRACTS\n")
    else:
        print("\n-- All data contracts exist",count)
    
    # list_of_dc_bcp = list_dc_bcp_name()
    # add_debug("\n== check DC and BCP name")
    # [add_debug(x) for x in list_of_dc_bcp]


    write_debug("step-0-debug-data-contracts-lookup.txt",debug)


if __name__ == "__main__":
    create_data_contracts_lookup()
