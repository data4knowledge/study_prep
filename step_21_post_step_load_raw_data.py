import os
import json
import csv
from pathlib import Path
from d4kms_service import Neo4jConnection
from model.configuration import Configuration, ConfigurationNode
import pandas as pd


def write_tmp(name, data):
    TMP_PATH = Path.cwd() / "tmp" / "saved_debug"
    OUTPUT_FILE = TMP_PATH / name
    print("Writing file...",OUTPUT_FILE.name,OUTPUT_FILE, end="")
    with open(OUTPUT_FILE, 'w') as f:
        for it in data:
            f.write(str(it))
            f.write('\n')
    print(" ...done")

def output_json(path, name, data):
    OUTPUT_FILE = path / f"{name}.json"
    if OUTPUT_FILE.exists():
        os.unlink(OUTPUT_FILE)
    print("Saving to",OUTPUT_FILE)
    with open(OUTPUT_FILE, 'w') as f:
        f.write(json.dumps(data, indent = 2))

def output_csv(path, name, data):
    OUTPUT_FILE = path / f"{name}.csv"
    if OUTPUT_FILE.exists():
        os.unlink(OUTPUT_FILE)
    print("Saving to",OUTPUT_FILE)
    output_variables = list(data[0].keys())
    with open(OUTPUT_FILE, 'w') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=output_variables)
        writer.writeheader()
        writer.writerows(data)

    # OUTPUT_XLSX = path / f"{name}.xlsx"
    # df = pd.DataFrame(data)
    # df.to_excel(OUTPUT_XLSX, index = False)
    # print("saved xlsx: ",OUTPUT_XLSX)

def save_file(path: Path, name, data):
    # output_json(path, name, data)
    output_csv(path, name, data)


def clear_created_nodes():
    db = Neo4jConnection()
    with db.session() as session:
        query = "match (n:Datapoint|DataPoint) detach delete n return count(n)"
        results = session.run(query)
        print("Removing Datapoint/DataPoint",results.data())
        query = "match (n:Subject) detach delete n return count(n)"
        results = session.run(query)
        print("Removing Subject",results.data())
    db.close()
    
def get_import_directory():
    db = Neo4jConnection()
    with db.session() as session:
        query = "call dbms.listConfig()"
        results = session.run(query)
        config = [x.data() for x in results]
        import_directory = next((item for item in config if item["name"] == 'server.directories.import'), None)
        return import_directory['value']


def copy_file_to_db_import(source, import_directory):
    target_folder = Path(import_directory)
    assert target_folder.exists(), f"Change Neo4j db import directory: {target_folder}"
    target_file = target_folder / source.name
    with open(source,'r') as f:
        txt = f.read()
    with open(target_file,'w') as f:
        f.write(txt)
    print("Written",target_file)

def copy_files_to_db_import(import_directory):
    enrolment_file = Path.cwd() / "data" / "output" / "enrolment_msg.csv"
    assert enrolment_file.exists(), f"enrolment_file does not exist: {enrolment_file}"
    copy_file_to_db_import(enrolment_file, import_directory)
    datapoints_file = Path.cwd() / "data" / "output" / "datapoints_msg.csv"
    assert datapoints_file.exists(), f"datapoints_file does not exist: {datapoints_file}"
    copy_file_to_db_import(datapoints_file, import_directory)

def add_identifiers():
    db = Neo4jConnection()
    with db.session() as session:
        query = """
            LOAD CSV WITH HEADERS FROM 'file:///enrolment_msg.csv' AS site_row
            MATCH (design:StudyDesign {name:'Study Design 1'})
            MERGE (s:Subject {identifier:site_row['USUBJID']})
            MERGE (site:StudySite {name:site_row['SITEID']})
            MERGE (s)-[:ENROLLED_AT_SITE_REL]->(site)
            MERGE (site)<-[:MANAGES_SITE]-(researchOrg)
            MERGE (researchOrg)<-[:ORGANIZATIONS_REL]-(design)
            RETURN count(*) as count
        """
        results = session.run(query)
        res = [result.data() for result in results]
    db.close()
    print("results datapoints",res)

def get_bc_properties():
    query = f"""
        MATCH (bc:BiomedicalConcept)-[:PROPERTIES_REL]->(bcp)<-[:PROPERTIES_REL]-(dc:DataContract)-[:INSTANCES_REL]->(act_inst:ScheduledActivityInstance)-[:ENCOUNTER_REL]-(enc)
        return bc.label as BC_LABEL, bcp.name as BCP_NAME, bcp.label as BCP_LABEL, enc.label as ENCOUNTER_LABEL, dc.uri as DC_URI
    """
    query = """
        MATCH(study:Study{name:'Study_CDISC PILOT - LZZT'})-[r1:VERSIONS_REL]->(StudyVersion)-[r2:STUDY_DESIGNS_REL]->(sd:StudyDesign)
        MATCH(sd)-[r3:SCHEDULE_TIMELINES_REL]->(tl:ScheduleTimeline) where not (tl)<-[:TIMELINE_REL]-()
        MATCH(tl)-[r4:INSTANCES_REL]->(act_inst_main:ScheduledActivityInstance)-[r5:ACTIVITY_REL]->(act:Activity)-[r6:BIOMEDICAL_CONCEPT_REL]->(bc:BiomedicalConcept)-[r7:PROPERTIES_REL]->(bcp:BiomedicalConceptProperty)
        MATCH (bcp)<-[:PROPERTIES_REL]-(dc:DataContract)-[:INSTANCES_REL]->(act_inst_main)-[:ENCOUNTER_REL]-(enc)
        return distinct bc.label as BC_LABEL, bcp.name as BCP_NAME, bcp.label as BCP_LABEL, enc.label as ENCOUNTER_LABEL, dc.uri as DC_URI
    """
    db = Neo4jConnection()
    with db.session() as session:
        results = session.run(query)
        res = [result.data() for result in results]
    db.close()
    return res

def get_bc_properties_sub_timeline():
    query = """
        match (bc:BiomedicalConcept)-[:PROPERTIES_REL]->(bcp:BiomedicalConceptProperty)
        MATCH (bcp)<-[:PROPERTIES_REL]-(ignore_dc:DataContract)
        match (ignore_dc)-[:INSTANCES_REL]->(enc_msai:ScheduledActivityInstance)-[:ENCOUNTER_REL]->(enc:Encounter)
        with distinct bc.name as BC_NAME, bc.label as BC_LABEL, bcp.name as bcp_name, bcp.label as bcp_label, enc.label as ENCOUNTER_LABEL, enc_msai.uuid as enc_msai_uuid
        MATCH (msai:ScheduledActivityInstance {uuid: enc_msai_uuid})<-[:INSTANCES_REL]-(dc:DataContract)-[:INSTANCES_REL]->(sub_sai:ScheduledActivityInstance)
        MATCH (dc)-[:PROPERTIES_REL]->(bcp:BiomedicalConceptProperty {name: bcp_name})<-[:PROPERTIES_REL]-(bc:BiomedicalConcept {name: BC_NAME})
        MATCH (sub_sai)<-[:RELATIVE_FROM_SCHEDULED_INSTANCE_REL]-(t:Timing)
        WITH BC_NAME, BC_LABEL, bcp.name as BCP_NAME, bcp.label as BCP_LABEL, sub_sai.name as sub_sai_name, ENCOUNTER_LABEL, t.value as TIMEPOINT_VALUE, dc.uri as DC_URI
        return BC_NAME, BC_LABEL, BCP_NAME, BCP_LABEL, ENCOUNTER_LABEL, TIMEPOINT_VALUE, DC_URI
    """
    db = Neo4jConnection()
    with db.session() as session:
        results = session.run(query)
        res = [result.data() for result in results]
    db.close()
    return res

def get_bc_properties_ae():
    query = """
        MATCH (study:Study{name:'Study_CDISC PILOT - LZZT'})-[r1:VERSIONS_REL]->(StudyVersion)-[r2:STUDY_DESIGNS_REL]->(sd:StudyDesign)
        WITH sd
        match (sd)-[:SCHEDULE_TIMELINES_REL]->(tl:ScheduleTimeline {name:'Adverse Event Timeline'})-[:INSTANCES_REL]->(sai:ScheduledActivityInstance)
        match (sai:ScheduledActivityInstance)<-[:INSTANCES_REL]-(dc:DataContract)
        match (dc)-[:PROPERTIES_REL]->(bcp:BiomedicalConceptProperty)<-[:PROPERTIES_REL]-(bc:BiomedicalConcept)
        return bc.label as BC_LABEL, bcp.name as BCP_NAME, bcp.label as BCP_LABEL, dc.uri as DC_URI,"" as ENCOUNTER_LABEL
    """
    # print("ae query", query)
    db = Neo4jConnection()
    with db.session() as session:
        results = session.run(query)
        res = [result.data() for result in results]
    db.close()
    return res

def add_datapoints():
    db = Neo4jConnection()
    with db.session() as session:
        query = """
            LOAD CSV WITH HEADERS FROM 'file:///datapoints_msg.csv' AS data_row
            MATCH (dc:DataContract {uri:data_row['DC_URI']})
            MATCH (design:StudyDesign {name:'Study Design 1'})
            MERGE (d:DataPoint {uri: data_row['DATAPOINT_URI'], value: data_row['VALUE']})
            MERGE (record:Record {key:data_row['RECORD_KEY']})
            MERGE (s:Subject {identifier:data_row['USUBJID']})
            MERGE (dc)<-[:FOR_DC_REL]-(d)
            MERGE (d)-[:FOR_SUBJECT_REL]->(s)
            MERGE (d)-[:SOURCE]->(record)
            RETURN count(*) as count
        """
        results = session.run(query)
        res = [result.data() for result in results]
    db.close()
    print("results datapoints",res)

def add_identifiers_datapoints():
    db = Neo4jConnection()
    with db.session() as session:
        query = """
            LOAD CSV WITH HEADERS FROM 'file:///datapoints.csv'  AS data_row
            WITH data_row
            LOAD CSV WITH HEADERS FROM 'file:///enrolment.csv'  AS site_row 
            with data_row, site_row
            MATCH (dc:DataContract {uri:data_row['DC_URI']})
            MATCH (design:StudyDesign {name:'Study Design 1'})-[:ORGANIZATIONS_REL]->(researchOrg)
            MERGE (d:DataPoint {uri: data_row['DATAPOINT_URI'], value: data_row['VALUE']})
            MERGE (s:Subject {identifier:data_row['USUBJID']})
            MERGE (site:StudySite {name:site_row['SITEID']})
            MERGE (dc)<-[:FOR_DC_REL]-(d)
            MERGE (d)-[:FOR_SUBJECT_REL]->(s)
            MERGE (s)-[:ENROLLED_AT_SITE_REL]->(site)
            MERGE (site)<-[:MANAGES_REL]-(researchOrg)
            RETURN count(*)
        """
        results = session.run(query)
    db.close()
    print("results datapoints",results)

def check_data_contracts():
    db = Neo4jConnection()
    with db.session() as session:
        query = """
            LOAD CSV WITH HEADERS FROM 'file:///datapoints.csv' AS data_row
            RETURN distinct data_row['DC_URI'] as data_contract
        """
        results = session.run(query)
        items = [result.data() for result in results]
    db.close()
    with db.session() as session:
        for item in items:
            query = f"""
                MATCH (dc:DataContract {{uri:'{item['data_contract']}'}})
                WITH COUNT(dc) > 0  as dc_exists
                RETURN dc_exists as exist
            """
            # print(query)
            results = session.run(query)
            if results[0].data()['exist']:
                pass
            else:
                print("\n---\ndata_contract MISSING :",item['data_contract'])
    db.close()

def get_raw_data_file(file):
    df = pd.read_csv(file)
    df = df.fillna("")
    data = df.to_dict(orient="records")
    return data

def get_unique_activities(data):
    unique_activities = {}
    for item in data:
        if item['TIMEPOINT'] != "":
            activity = {'visit':item['VISIT'],'label':item['LABEL'],'variable':item['VARIABLE'],'timepoint':item['TIMEPOINT']}
            key = str(activity)
        elif item['VISIT'] != "":
            activity = {'visit':item['VISIT'],'label':item['LABEL'],'variable':item['VARIABLE']}
            key = str(activity)
        else:
            activity = {'label':item['LABEL'],'variable':item['VARIABLE']}
            key = str(activity)
        if not key in unique_activities:
            unique_activities[key] = activity
    return unique_activities

debug = []

def create_enrolment_file(raw_data):
    print("\ncreate enrolment file")
    subject_no = set([r['SUBJID'] for r in raw_data])

    # NOTE: Fix so that SUBJID is loaded, not USUBJID
    # Make it look like the previous load file
    subjects = []
    for subjid in subject_no:
        item = {}
        item['STUDY_URI'] = "https://study.d4k.dk/study-cdisc-pilot-lzzt"
        item['SITEID'] = "701"
        item['USUBJID'] = subjid
        subjects.append(item)

    save_file(OUTPUT_PATH,"enrolment_msg",subjects)

def create_datapoint_file(raw_data):
    print("\ncreate datapoint file")
    properties = get_bc_properties()
    debug.append(f"\n------- properties ({len(properties)})")
    # for r in properties: debug.append(r)

    # NOTE: Not all blood pressure measurements are repeated, so data contracts for SCREENING 1, SCREENING 2, BASELINEWEEK 2, WEEK 4, WEEK 6, WEEK 8
    # All records marked as baseline are STANDING VSREPNUM = 3 -> PT2M. So I'll use that for them
    properties_sub_timeline = get_bc_properties_sub_timeline()
    debug.append(f"\n------- properties_sub_timeline ({len(properties_sub_timeline)})")
    # for r in properties_sub_timeline[0:4]: debug.append(r)

    properties_ae = get_bc_properties_ae()
    debug.append(f"\n------- properties_ae ({len(properties_ae)})")
    # for r in properties_ae[0:10]: debug.append(r)
    for r in properties_ae: debug.append(r)
    
    write_tmp("step-21-post_step.txt",debug)

    print("\--get unique activities from raw_data")
    unique_activities = get_unique_activities(raw_data)
    debug.append(f"\n------- unique_activities ({len(unique_activities)})")
    # Reducing when debugging
    # unique_activities = dict(list(unique_activities.items())[0:4])

    missing = []
    datapoints = []
    debug.append("\n------- looping unique_activities")
    for k,v in unique_activities.items():
        rows = []
        if 'timepoint' in v:
            dc = next((i['DC_URI'] for i in properties_sub_timeline if i['BC_LABEL'] == v['label'] and i['ENCOUNTER_LABEL'] == v['visit'] and i['BCP_NAME'] == v['variable'] and i['TIMEPOINT_VALUE'] == v['timepoint']),[])
            if dc:
                rows = [r for r in raw_data if r['LABEL'] == v['label'] and r['VISIT'] == v['visit'] and r['VARIABLE'] == v['variable'] and r['TIMEPOINT'] == v['timepoint']]
        elif 'visit' in v:
            dc = next((i['DC_URI'] for i in properties if i['BC_LABEL'] == v['label'] and i['ENCOUNTER_LABEL'] == v['visit'] and i['BCP_LABEL'] == v['variable']),[])
            # NOTE: There is a mismatches within the BC specializations, So sometimes label matches, sometimes the name
            if not dc:
                dc = next((i['DC_URI'] for i in properties if i['BC_LABEL'] == v['label'] and i['ENCOUNTER_LABEL'] == v['visit'] and i['BCP_NAME'] == v['variable']),[])
            if dc:
                rows = [r for r in raw_data if r['LABEL'] == v['label'] and r['VISIT'] == v['visit'] and r['VARIABLE'] == v['variable']]
        else:
            # AE is the only thing that is not visit bound at the moment
            dc = next((i['DC_URI'] for i in properties_ae if i['BC_LABEL'] == v['label'] and i['BCP_NAME'] == v['variable']),[])
            if dc:
                rows = [r for r in raw_data if r['LABEL'] == v['label'] and r['VARIABLE'] == v['variable']]

        for row in rows:
            item = {}
            item['USUBJID'] = row['SUBJID']
            fixed_label = row['LABEL'].replace(" ","").replace("-","")
            fixed_variable = row['VARIABLE'].replace(" ","").replace("-","")
            thing = f"{fixed_label}/{fixed_variable}"
            if 'TIMEPOINT' in row and row['TIMEPOINT']:
                dp_uri = f"{dc}{row['SUBJID']}/{thing}/{row['ROW_NO']}/{row['TIMEPOINT']}"
                record_key = f"{fixed_label}/{row['SUBJID']}/{row['ROW_NO']}/{row['TIMEPOINT']}"
            elif 'VISIT' in row and row['VISIT']:
                dp_uri = f"{dc}{row['SUBJID']}/{thing}/{row['ROW_NO']}"
                record_key = f"{fixed_label}/{row['SUBJID']}/{row['ROW_NO']}"
            else:
                dp_uri = f"{dc}{row['SUBJID']}/{thing}/{row['ROW_NO']}"
                record_key = f"{fixed_label}/{row['SUBJID']}/{row['ROW_NO']}"
            item['DATAPOINT_URI'] = dp_uri
            item['VALUE'] = row['VALUE']
            item['DC_URI'] = dc
            item['RECORD_KEY'] = record_key
            datapoints.append(item)

        if not dc:
            missing.append(v)

    debug.append("\n------- building datapoints")
    for r in datapoints[0:5]:
        debug.append(r)


    debug.append("\n------- missing")
    metadata = {}
    for r in missing:
        key = f"{r['visit']}-{r['timepoint']}" if 'timepoint' in r else r['visit']
        if key in metadata:
            metadata[key].append(f"{r['label']}.{r['variable']}")
        else:
            metadata[key] = [f"{r['label']}.{r['variable']}"]

    for r in metadata.items():
        debug.append(r)

    write_tmp("step-21-post_step.txt",debug)

    save_file(OUTPUT_PATH,"datapoints_msg",datapoints)

def load_datapoints():
    print("\nload to db")
    clear_created_nodes()
    import_directory = get_import_directory()
    print("\n neo4j import directory", import_directory)
    copy_files_to_db_import(import_directory)

    print("\nadd identifiers")
    add_identifiers()

    print("\nadd datapoints")
    add_datapoints()
    c = Configuration()
    # ConfigurationNode.delete()
    ConfigurationNode.create(c._configuration)


OUTPUT_PATH = Path.cwd() / "data" / "output"
assert OUTPUT_PATH.exists(), "OUTPUT_PATH not found"


def main():
    print("\nget raw data")
    raw_data_file = Path.cwd() / "data" / "output" / "raw_data.csv"
    assert raw_data_file.exists(), f"raw_data_file does not exist: {raw_data_file}"
    raw_data = get_raw_data_file(raw_data_file)
    debug.append(f"\n------- raw data ({len(raw_data)})")
    for r in raw_data[0:5]:
        debug.append(r)

    create_enrolment_file(raw_data)
    create_datapoint_file(raw_data)
    load_datapoints()

if __name__ == "__main__":
    main()