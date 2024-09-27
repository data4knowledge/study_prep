import json
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


def clear_created_nodes():
    db = Neo4jConnection()
    with db.session() as session:
        query = "match (n:Datapoint|DataPoint) detach delete n return count(n)"
        results = session.run(query)
        print("Removing Datapoint/DataPoint",results)
        query = "match (n:Subject) detach delete n return count(n)"
        results = session.run(query)
        print("Removing Subject",results)
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
    enrolment_file = Path.cwd() / "data" / "output" / "enrolment.csv"
    assert enrolment_file.exists(), f"enrolment_file does not exist: {enrolment_file}"
    copy_file_to_db_import(enrolment_file, import_directory)
    datapoints_file = Path.cwd() / "data" / "output" / "datapoints.csv"
    assert datapoints_file.exists(), f"datapoints_file does not exist: {datapoints_file}"
    copy_file_to_db_import(datapoints_file, import_directory)
    row_datapoints_file = Path.cwd() / "data" / "output" / "row_datapoints.csv"
    assert row_datapoints_file.exists(), f"row_datapoints_file does not exist: {row_datapoints_file}"
    copy_file_to_db_import(row_datapoints_file, import_directory)

def add_identifiers():
    db = Neo4jConnection()
    with db.session() as session:
        query = """
            LOAD CSV WITH HEADERS FROM 'file:///enrolment.csv' AS site_row
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

# def xx_get_bc_properties_sub_timeline(bc_label, tpt, row):
#     if row['VISIT'] in DATA_VISITS_TO_ENCOUNTER_LABELS:
#         visit = DATA_VISITS_TO_ENCOUNTER_LABELS[row['VISIT']]
#     else:
#         add_issue("visit not found:",row['VISIT'])
#         return []
#     query = f"""
#         match (msai:ScheduledActivityInstance)<-[:INSTANCES_REL]-(dc:DataContract)-[:INSTANCES_REL]-(ssai:ScheduledActivityInstance)
#         match (msai)-[:ENCOUNTER_REL]->(enc:Encounter)
#         match (ssai)<-[:RELATIVE_FROM_SCHEDULED_INSTANCE_REL]-(t:Timing)
#         match (dc)-[:PROPERTIES_REL]->(bcp:BiomedicalConceptProperty)<-[:PROPERTIES_REL]-(bc:BiomedicalConcept)
#         WHERE enc.label = '{visit}'
#         and    t.value = '{tpt}'
#         AND  bc.label = '{bc_label}'
#         return bc.label as BC_LABEL, bcp.name as BCP_NAME, bcp.label as BCP_LABEL, enc.label as ENCOUNTER_LABEL, t.value as TIMEPOINT_VALUE, dc.uri as DC_URI
#     """
#     results = db_query(query)
#     if results == []:
#         add_issue("timeline DataContract query has errors in it",visit,bc_label,tpt,query)
#         return []
#     else:
#         return results



def create_data_contracts1():
    query = """
        MATCH(study:Study{name:'Study_CDISC PILOT - LZZT'})-[r1:VERSIONS_REL]->(StudyVersion)-[r2:STUDY_DESIGNS_REL]->(sd:StudyDesign)
        MATCH(sd)-[r3:SCHEDULE_TIMELINES_REL]->(tl:ScheduleTimeline) where not (tl)<-[:TIMELINE_REL]-()
        MATCH(tl)-[r4:INSTANCES_REL]->(act_inst_main:ScheduledActivityInstance)-[r5:ACTIVITY_REL]->(act:Activity)-[r6:BIOMEDICAL_CONCEPT_REL]->(bc:BiomedicalConcept)-[r7:PROPERTIES_REL]->(bc_prop:BiomedicalConceptProperty) 
        return distinct study, sd, tl, act, act_inst_main, act_inst_main.uuid as act_inst_uuid, null as act_inst, bc,bc_prop
    """
    db = Neo4jConnection()
    with db.session() as session:
        results = session.run(query)
        res = [result.data() for result in results]
    db.close()
    return res

def create_data_contracts2():
    query = """
        MATCH(study:Study{name:'Study_CDISC PILOT - LZZT'})-[r1:VERSIONS_REL]->(StudyVersion)-[r2:STUDY_DESIGNS_REL]->(sd:StudyDesign)
        MATCH(sd)-[r3:SCHEDULE_TIMELINES_REL]->(tl:ScheduleTimeline)<-[r4:TIMELINE_REL]-(act_main)<-[r5:ACTIVITY_REL]-(act_inst_main:ScheduledActivityInstance)
        MATCH(tl)-[r6:INSTANCES_REL]->(act_inst:ScheduledActivityInstance)-[r7:ACTIVITY_REL]->(act:Activity)-[r8:BIOMEDICAL_CONCEPT_REL]->(bc:BiomedicalConcept)-[r9:PROPERTIES_REL]->(bc_prop:BiomedicalConceptProperty) 
        return distinct study, sd, tl, act, act_inst_main, act_inst_main.uuid+'/'+ act_inst.uuid as act_inst_uuid, act_inst, bc,bc_prop
    """
    db = Neo4jConnection()
    with db.session() as session:
        results = session.run(query)
        res = [result.data() for result in results]
    db.close()
    return res

def get_bc_properties():
    query = f"""
        MATCH (bc:BiomedicalConcept)-[:PROPERTIES_REL]->(bcp)<-[:PROPERTIES_REL]-(dc:DataContract)-[:INSTANCES_REL]->(act_inst:ScheduledActivityInstance)-[:ENCOUNTER_REL]-(enc)
        return bc.label as BC_LABEL, bcp.name as BCP_NAME, bcp.label as BCP_LABEL, enc.label as ENCOUNTER_LABEL, dc.uri as DC_URI
    """
    query = """
        MATCH(study:Study{name:'Study_CDISC PILOT - LZZT'})-[r1:VERSIONS_REL]->(StudyVersion)-[r2:STUDY_DESIGNS_REL]->(sd:StudyDesign)
        MATCH(sd)-[r3:SCHEDULE_TIMELINES_REL]->(tl:ScheduleTimeline) where not (tl)<-[:TIMELINE_REL]-()
        MATCH(tl)-[r4:INSTANCES_REL]->(act_inst_main:ScheduledActivityInstance)-[r5:ACTIVITY_REL]->(act:Activity)-[r6:BIOMEDICAL_CONCEPT_REL]->(bc:BiomedicalConcept)-[r7:PROPERTIES_REL]->(bc_prop:BiomedicalConceptProperty)
        MATCH (bcp)<-[:PROPERTIES_REL]-(dc:DataContract)-[:INSTANCES_REL]->(act_inst_main)-[:ENCOUNTER_REL]-(enc)
        return distinct bc.label as BC_LABEL, bcp.name as BCP_NAME, bcp.label as BCP_LABEL, enc.label as ENCOUNTER_LABEL, dc.uri as DC_URI
    """
    db = Neo4jConnection()
    with db.session() as session:
        results = session.run(query)
        res = [result.data() for result in results]
    db.close()
    return res

# def get_bc_properties_dm():
#     query = f"""
#         MATCH (bc:BiomedicalConcept)-[:PROPERTIES_REL]->(bcp)<-[:PROPERTIES_REL]-(dc:DataContract)-[:INSTANCES_REL]->(act_inst)-[:ENCOUNTER_REL]-(enc)
#         WHERE  bc.label = '{bc_label}'
#         return bc.label as BC_LABEL, bcp.name as BCP_NAME, bcp.label as BCP_LABEL, enc.label as ENCOUNTER_LABEL, dc.uri as DC_URI
#     """
#     db = Neo4jConnection()
#     with db.session() as session:
#         results = session.run(query)
#         res = [result.data() for result in results]
#     db.close()
#     return res


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
        # WHERE bc.label = '{bc_label}'
    query = f"""
        match (msai:ScheduledActivityInstance)<-[:INSTANCES_REL]-(dc:DataContract)-[:INSTANCES_REL]-(ssai:ScheduledActivityInstance)
        match (ssai)<-[:RELATIVE_FROM_SCHEDULED_INSTANCE_REL]-(t:Timing)
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

def test_datapoints():
    db = Neo4jConnection()
    with db.session() as session:
        query = """
            LOAD CSV WITH HEADERS FROM 'file:///datapoints.csv' AS data_row
            MATCH (dc:DataContract {uri:data_row['DC_URI']})
            MATCH (design:StudyDesign {name:'Study Design 1'})
            MERGE (d:DataPoint {uri: data_row['DATAPOINT_URI'], value: data_row['VALUE']})
            MERGE (s:Subject {identifier:data_row['USUBJID']})
            MERGE (dc)<-[:FOR_DC_REL]-(d)
            MERGE (d)-[:FOR_SUBJECT_REL]->(s)
            RETURN count(*) as count
        """
        # {'ROW_NO': '1.0', 'VARIABLE': 'VSORRES', 'VALUE': '64', 'LABEL': 'Diastolic Blood Pressure', 'VISIT': 'Screening 1', 'TIMEPOINT': 'PT5M', 'SUBJID': '01-701-1015'}}
        # SUBJID,ROW_NO,VISIT,VARIABLE,LABEL,TIMEPOINT,VALUE

        query = """
            match (msai:ScheduledActivityInstance)<-[:INSTANCES_REL]-(dc:DataContract)-[:INSTANCES_REL]-(ssai:ScheduledActivityInstance)
            match (msai)-[:ENCOUNTER_REL]->(enc:Encounter)
            match (ssai)<-[:RELATIVE_FROM_SCHEDULED_INSTANCE_REL]-(t:Timing)
            match (dc)-[:PROPERTIES_REL]->(bcp:BiomedicalConceptProperty)<-[:PROPERTIES_REL]-(bc:BiomedicalConcept)
            return bc.label as BC_LABEL, bcp.name as BCP_NAME, bcp.label as BCP_LABEL, enc.label as ENCOUNTER_LABEL, t.value as TIMEPOINT_VALUE, dc.uri as DC_URI
        """ % ()
        # From study service create data contract sub timeline
        query = """
            MATCH(study:Study{name:'Study_CDISC PILOT - LZZT'})-[r1:VERSIONS_REL]->(StudyVersion)-[r2:STUDY_DESIGNS_REL]->(sd:StudyDesign)
            MATCH(sd)-[r3:SCHEDULE_TIMELINES_REL]->(tl:ScheduleTimeline)<-[r4:TIMELINE_REL]-(act_main)<-[r5:ACTIVITY_REL]-(act_inst_main:ScheduledActivityInstance)
            MATCH(tl)-[r6:INSTANCES_REL]->(act_inst:ScheduledActivityInstance)-[r7:ACTIVITY_REL]->(act:Activity)-[r8:BIOMEDICAL_CONCEPT_REL]->(bc:BiomedicalConcept)-[r9:PROPERTIES_REL]->(bc_prop:BiomedicalConceptProperty) 
            where bc.label = "Diastolic Blood Pressure"
            and   bc_prop.name = "VSORRES"
            return distinct study, sd, tl, act, act_inst_main, act_inst_main.uuid+'/'+ act_inst.uuid as act_inst_uuid, act_inst, bc,bc_prop
        """ % ()
        query = """
call apoc.cypher.run("MATCH(study:Study{name:$s})-[r1:VERSIONS_REL]->(StudyVersion)-[r2:STUDY_DESIGNS_REL]->(sd:StudyDesign)
MATCH(sd)-[r3:SCHEDULE_TIMELINES_REL]->(tl:ScheduleTimeline) where not (tl)<-[:TIMELINE_REL]-()
MATCH(tl)-[r4:INSTANCES_REL]->(act_inst_main:ScheduledActivityInstance)-[r5:ACTIVITY_REL]->(act:Activity)-[r6:BIOMEDICAL_CONCEPT_REL]->(bc:BiomedicalConcept)-[r7:PROPERTIES_REL]->(bc_prop:BiomedicalConceptProperty) 
WHERE bc.name = 'Diastolic Blood Pressure'
and   bc_prop.name = 'VSORRES'
return distinct study, 
sd, 
tl, 
act, 
act_inst_main,
act_inst_main.uuid as act_inst_uuid,
null as act_inst,  
bc,
bc_prop
      UNION
MATCH(study:Study{name:$s})-[r1:VERSIONS_REL]->(StudyVersion)-[r2:STUDY_DESIGNS_REL]->(sd:StudyDesign)
MATCH(sd)-[r3:SCHEDULE_TIMELINES_REL]->(tl:ScheduleTimeline)<-[r4:TIMELINE_REL]-(act_main)<-[r5:ACTIVITY_REL]-(act_inst_main:ScheduledActivityInstance)
MATCH(tl)-[r6:INSTANCES_REL]->(act_inst:ScheduledActivityInstance)-[r7:ACTIVITY_REL]->(act:Activity)-[r8:BIOMEDICAL_CONCEPT_REL]->(bc:BiomedicalConcept)-[r9:PROPERTIES_REL]->(bc_prop:BiomedicalConceptProperty) 
WHERE bc.name = 'Diastolic Blood Pressure'
and   bc_prop.name = 'VSORRES'
return distinct study, 
sd, 
tl, 
act, 
act_inst_main,
act_inst_main.uuid+'/'+ act_inst.uuid as act_inst_uuid,
act_inst,  
bc,
bc_prop",{s:'Study_CDISC PILOT - LZZT'}) YIELD value
WITH value.study as study, 
value.sd as sd, 
value.tl as tl, 
value.act as act, 
value.act_inst_main as act_inst_main, 
value.act_inst as act_inst, 
value.bc as bc, 
value.bc_prop as bc_prop, 
value. act_inst_uuid as  act_inst_uuid
WITH study, sd, tl,act,act_inst_main,act_inst,bc,bc_prop,act_inst_uuid
return study.name as study_name, sd.name as sd_name, tl.name as tl_name, act.name as act_name, act_inst_main.name as act_inst_main_name, act_inst.name as act_inst_name, bc.name as bc_name, bc_prop.name as bc_prop_name, act_inst_uuid
        """ % ()
        # My new fantastic
        query = """
match (bc:BiomedicalConcept)-[:PROPERTIES_REL]->(bcp:BiomedicalConceptProperty)
MATCH (bcp)<-[:PROPERTIES_REL]-(ignore_dc:DataContract)
match (ignore_dc)-[:INSTANCES_REL]->(enc_msai:ScheduledActivityInstance)-[:ENCOUNTER_REL]->(enc:Encounter)
with distinct bc.name as BC_NAME, bc.label as BC_LABEL, bcp.name as bcp_name, bcp.label as bcp_label, enc.label as ENCOUNTER_LABEL, enc_msai.uuid as enc_msai_uuid
MATCH (msai:ScheduledActivityInstance {uuid: enc_msai_uuid})<-[:INSTANCES_REL]-(dc:DataContract)-[:INSTANCES_REL]->(sub_sai:ScheduledActivityInstance)
MATCH (dc)-[:PROPERTIES_REL]->(bcp:BiomedicalConceptProperty {name: bcp_name})<-[:PROPERTIES_REL]-(bc:BiomedicalConcept {name: BC_NAME})
MATCH (sub_sai)<-[:RELATIVE_FROM_SCHEDULED_INSTANCE_REL]-(t:Timing)
WITH BC_NAME, BC_LABEL, bcp.name as BCP_NAME, bcp.label as BCP_LABEL, sub_sai.name as sub_sai_name, ENCOUNTER_LABEL, t.value as TIMEPOINT_VALUE, dc.uri as DC_URI
return BC_NAME, BCP_NAME, BCP_LABEL, sub_sai_name, ENCOUNTER_LABEL, TIMEPOINT_VALUE, DC_URI
        """ % ()
        print("test query\n",query)
        results = session.run(query)
        res = [result.data() for result in results]
    db.close()
    return res

def add_datapoints(activity):
    if 'timepoint' in activity:
        # activity = {'visit':item['VISIT'],'label':item['LABEL'],'variable':item['VARIABLE'],'timepoint':item['TIMEPOINT']}
        query = """
            LOAD CSV WITH HEADERS FROM 'file:///datapoints.csv' AS row
            with row
            WHERE row['VISIT'] = '%s' and row['LABEL'] = '%s' and row['VARIABLE'] = '%s' and row['TIMEPOINT'] = '%s'
            match (msai:ScheduledActivityInstance)<-[:INSTANCES_REL]-(dc:DataContract)-[:INSTANCES_REL]-(ssai:ScheduledActivityInstance)
            match (msai)-[:ENCOUNTER_REL]->(enc:Encounter)
            match (ssai)<-[:RELATIVE_FROM_SCHEDULED_INSTANCE_REL]-(t:Timing)
            match (dc)-[:PROPERTIES_REL]->(bcp:BiomedicalConceptProperty)<-[:PROPERTIES_REL]-(bc:BiomedicalConcept)
            WHERE enc.label = row['VISIT']
            and   t.value = row['TIMEPOINT']
            AND   bc.label = row['LABEL']
            WITH dc.uri as dc_uri, row['VALUE'] as value, row['SUBJID'] as subjid, dc.uri+'/'+row['SUBJID']+'/'+row['ROW_NO'] as dp_uri
            MATCH (dc2 {uri: dc_uri})
            MERGE (d:DataPoint {uri: dp_uri, value: value})
            MERGE (s:Subject {identifier: subjid})
            MERGE (dc)<-[:FOR_DC_REL]-(d)
            MERGE (d)-[:FOR_SUBJECT_REL]->(s)
            return count(*)
        """ % (activity['visit'],activity['label'],activity['variable'],activity['timepoint'])
    else:
        # activity = {'visit':item['VISIT'],'label':item['LABEL'],'variable':item['VARIABLE']}
        query = """
            LOAD CSV WITH HEADERS FROM 'file:///datapoints.csv' AS row
            WHERE row['VISIT'] = 'Screening 1' and row['LABEL'] = 'Diastolic Blood Pressure' and row['VARIABLE'] = 'VSORRES' and row['TIMEPOINT'] = 'PT5M'
            match (msai:ScheduledActivityInstance)<-[:INSTANCES_REL]-(dc:DataContract)-[:INSTANCES_REL]-(ssai:ScheduledActivityInstance)
            match (msai)-[:ENCOUNTER_REL]->(enc:Encounter)
            match (ssai)<-[:RELATIVE_FROM_SCHEDULED_INSTANCE_REL]-(t:Timing)
            match (dc)-[:PROPERTIES_REL]->(bcp:BiomedicalConceptProperty)<-[:PROPERTIES_REL]-(bc:BiomedicalConcept)
            WHERE enc.label = row['VISIT']
            and   t.value = row['TIMEPOINT']
            AND   bc.label = row['LABEL']
            WITH dc.uri as dc_uri, row['VALUE'] as value, row['SUBJID'] as subjid, dc.uri+'/'+row['SUBJID']+'/'+row['ROW_NO'] as dp_uri
            MATCH (dc2 {uri: dc_uri})
            MERGE (d:DataPoint {uri: dp_uri, value: value})
            MERGE (s:Subject {identifier: subjid})
            MERGE (dc)<-[:FOR_DC_REL]-(d)
            MERGE (d)-[:FOR_SUBJECT_REL]->(s)
            return count(*)
        """
        query = """MATCH (n:DataPoint) return count(*) as count"""
    debug.append(str(activity))
    debug.append(query)
    print("datapoint query\n",query)

    db = Neo4jConnection()
    with db.session() as session:
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
def link_row_datapoints():
    # print("\nGetting row datapoints")
    # DP_FILE = Path.cwd() / "data" / "output" / "row_datapoints.json"
    # assert DP_FILE.exists(), "DP_FILE not found"
    # with open(DP_FILE) as f:
    #     data = json.load(f)

    db = Neo4jConnection()
    with db.session() as session:
        query = """
            LOAD CSV WITH HEADERS FROM 'file:///row_datapoints.csv'  AS data_row
            WITH data_row
            MATCH (dp:DataPoint {uri:data_row['datapoint_uri']})
            MERGE (record:Record {key:data_row['key']})
            MERGE (dp)-[:SOURCE]->(record)
            RETURN count(*)
        """
        print("query",query)
        results = session.run(query)
        print("results row datapoints",[result.data() for result in results])
    db.close()

def get_datapoints_file(file):
    df = pd.read_csv(file)
    df = df.fillna("")
    data = df.to_dict(orient="records")
    return data

def get_unique_activities(data):
    # SUBJID,ROW_NO,VISIT,VARIABLE,LABEL,TIMEPOINT,VALUE
    unique_activities = {}
    for item in data:
        # if item['TIMEPOINT'] != "":
        if item['TIMEPOINT'] != "":
            activity = {'visit':item['VISIT'],'label':item['LABEL'],'variable':item['VARIABLE'],'timepoint':item['TIMEPOINT']}
            key = str(activity)
        else:
            activity = {'visit':item['VISIT'],'label':item['LABEL'],'variable':item['VARIABLE']}
            key = str(activity)
        if not key in unique_activities:
            unique_activities[key] = activity
    return unique_activities

debug = []
matches = []
issues = []


def load_datapoints():
    clear_created_nodes()
    import_directory = get_import_directory()
    copy_files_to_db_import(import_directory)
    # add_identifiers_datapoints()
    print("\nadd identifiers")
    add_identifiers()

    print("\nget distincts")
    datapoints_file = Path.cwd() / "data" / "output" / "datapoints.csv"
    assert datapoints_file.exists(), f"datapoints_file does not exist: {datapoints_file}"
    data = get_datapoints_file(datapoints_file)
    debug.append(f"\n------- data ({len(data)})")

    # dia_data = [r for r in data if r['LABEL'] == "Diastolic Blood Pressure" and r['VARIABLE'] == "VSORRES"]
    # debug.append("\n------- dia_data")
    # for r in dia_data:
    #     debug.append(r)

    properties = get_bc_properties()
    debug.append(f"\n------- properties ({len(properties)})")

    # debug.append("\n------- dbp_properties")
    # dbp_properties = [r for r in properties if r['BC_LABEL'] == "Diastolic Blood Pressure" and r['BCP_LABEL'] == "VSORRES"]
    # for r in dbp_properties:
    #     debug.append(r)

    # debug.append("\n------- properties")
    # for r in properties[0:20]:
    #     debug.append(r)

    # properties_dm = get_bc_properties_dm()

    # NOTE: Not all blood pressure measurements are repeated, so data contracts for SCREENING 1, SCREENING 2, BASELINEWEEK 2, WEEK 4, WEEK 6, WEEK 8
    # All records marked as baseline are STANDING VSREPNUM = 3 -> PT2M. So I'll use that for them
    properties_sub_timeline = get_bc_properties_sub_timeline()
    debug.append(f"\n------- properties_sub_timeline ({len(properties_sub_timeline)})")
    for r in properties_sub_timeline[0:4]:
        debug.append(r)
    # properties_sub_timeline = [r for r in properties_sub_timeline if r['BC_LABEL'] == "Diastolic Blood Pressure"]
    # debug.append(f"len(properties_sub_timeline): {len(properties_sub_timeline)}")
    # debug.append("------- properties")

    # debug.append("\n------- test")
    # res = test_datapoints()
    # for r in res[0:5]:
    #     debug.append(r)

    write_tmp("step-21-post_step.txt",debug)

    print("\get unique activities from data")
    unique_activities = get_unique_activities(data)
    debug.append(f"\n------- unique_activities ({len(unique_activities)})")
    # debug.append("\n------- unique_activities before list(set)")
    # for r in unique_activities:
    #     debug.append(r)

    # Reducing when debugging
    # unique_activities = dict(list(unique_activities.items())[0:4])


    # encounter = 'Baseline'
    # encounter = 'Screening 1'
    # bcp_label = 'Date Time'
    # tpt = 'PT1M'
    # # acts_for_dia_sub = [i for i in properties_sub_timeline if i['BC_LABEL'] == 'Diastolic Blood Pressure' and i['ENCOUNTER_LABEL'] == encounter and i['BCP_LABEL'] == bcp_label and i['TIMEPOINT_VALUE'] == tpt]
    # acts_for_dia_sub = [i for i in properties_sub_timeline if i['BC_LABEL'] == 'Diastolic Blood Pressure' and i['BCP_LABEL'] == bcp_label]
    # debug.append("\n------- acts_for_dia_sub")
    # for r in acts_for_dia_sub:
    #     debug.append(r)


    # acts_for_dia = [i for i in properties if i['BC_LABEL'] == 'Diastolic Blood Pressure' and i['ENCOUNTER_LABEL'] == encounter and i['BCP_LABEL'] == bcp_label]
    # # acts_for_dia = [i for i in properties if i['BC_LABEL'] == 'Diastolic Blood Pressure' and i['ENCOUNTER_LABEL'] == encounter]
    # debug.append("\n------- acts_for_dia")
    # for r in acts_for_dia:
    #     debug.append(r)


    missing = []
    matches = []
    # # res =               dict(list(test_dict.items())[0: K])
    debug.append("\n------- looping unique_activities")
    for k,v in unique_activities.items():
        if 'timepoint' in v:
            # dc = [i for i in properties_sub_timeline if i['BC_LABEL'] == v['label'] and i['ENCOUNTER_LABEL'] == v['visit'] and i['BCP_LABEL'] == v['variable'] and i['TIMEPOINT_VALUE'] == v['timepoint']]
            # dc = next((i for i in properties_sub_timeline if i['BC_LABEL'] == v['label'] and i['ENCOUNTER_LABEL'] == v['visit'] and i['BCP_LABEL'] == v['variable'] and i['TIMEPOINT_VALUE'] == v['timepoint']),[])
            dc = next((i for i in properties_sub_timeline if i['BC_LABEL'] == v['label'] and i['ENCOUNTER_LABEL'] == v['visit'] and i['BCP_NAME'] == v['variable'] and i['TIMEPOINT_VALUE'] == v['timepoint']),[])
        else:
            # dc = [i for i in properties if i['BC_LABEL'] == v['label'] and i['ENCOUNTER_LABEL'] == v['visit'] and i['BCP_LABEL'] == v['variable']]
            dc = next((i for i in properties if i['BC_LABEL'] == v['label'] and i['ENCOUNTER_LABEL'] == v['visit'] and i['BCP_LABEL'] == v['variable']),[])
        # debug.append(f"len(dc): {len(dc)}")
        if not dc:
            # v.pop('variable')
            # v.pop('label')
            missing.append(v)
            debug.append(f" -- missing {v}")
        else:
            matches.append(v)
            debug.append(f"{v}")
        # debug.append(f"dc: {dc}")
    #     add_datapoints(v)
    debug.append("\n------- missing")
    metadata = {}
    for r in missing:
        key = f"{r['visit']}-{r['timepoint']}" if 'timepoint' in r else r['visit']
        debug.append(f"next key {key} {key in metadata}")

        if key in metadata:
            debug.append(f"  updating key {key} with {r['label']}.{r['variable']}")
            metadata[key].append(f"{r['label']}.{r['variable']}")
        else:
            debug.append(f"  adding key {key} {r['label']}.{r['variable']}")
            metadata[key] = [f"{r['label']}.{r['variable']}"]

    # u_missing = list({v.values():v for v in missing}.values())
    # u_missing = set([str(v) for v in missing])
    for r in metadata.items():
        debug.append(r)

    write_tmp("step-21-post_step.txt",debug)

    return
    # check_data_contracts()
    print("\nlink row datatpoints")
    link_row_datapoints()
    c = Configuration()
    # ConfigurationNode.delete()
    ConfigurationNode.create(c._configuration)

if __name__ == "__main__":
    load_datapoints()
