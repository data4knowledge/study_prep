import json
# import pandas as pd
from pathlib import Path
from neo4j import GraphDatabase
from d4kms_service import Neo4jConnection, ServiceEnvironment
from utility.neo_utils import db_is_down
from utility.mappings import DATA_LABELS_TO_BC_LABELS, DATA_VISITS_TO_ENCOUNTER_LABELS, DATA_TPT_TO_TIMING_LABELS
from utility.debug import write_debug, write_tmp

debug = []

vs_query = """
      match(domain:Domain{name:'VS'})-[:VARIABLE_REL]->(var:Variable)-[:IS_A_REL]->(crm:CRMNode)<-[:IS_A_REL]-(bc_prop:BiomedicalConceptProperty), (domain)-[:USING_BC_REL]->(bc:BiomedicalConcept)-[:PROPERTIES_REL]->(bc_prop)<-[:PROPERTIES_REL]-(dc:DataContract)-[:INSTANCES_REL]->(act_inst_main:ScheduledActivityInstance)<-[:RELATIVE_FROM_SCHEDULED_INSTANCE_REL]-(tim:Timing)-[:TYPE_REL]->(tim_ref:Code)
      OPTIONAL MATCH(act_inst_main)-[:ENCOUNTER_REL]->(e:Encounter),(act_inst_main)-[:EPOCH_REL]->(epoch:StudyEpoch)
      OPTIONAL MATCH(act_inst_main)<-[:INSTANCES_REL]-(tl:ScheduleTimeline)
      MATCH (domain)<-[:DOMAIN_REL]-(sd:StudyDesign)<-[:STUDY_DESIGNS_REL]-(sv:StudyVersion)-[:STUDY_IDENTIFIERS_REL]->(si:StudyIdentifier)-[:STUDY_IDENTIFIER_SCOPE_REL]->(sis:Organization {name:'Eli Lilly'})
      MATCH (dc)<-[:FOR_DC_REL]-(d:DataPoint)-[:FOR_SUBJECT_REL]->(s:Subject)
      MATCH (bc)-[:CODE_REL]->()-[:STANDARD_CODE_REL]-(code:Code)
      WITH code.decode as test_code, si,domain, collect(epoch.name) as epoch,collect(toInteger(split(e.id,'_')[1])) as e_order,var, bc, dc, d, s, collect(e.label) as vis, apoc.map.fromPairs(collect([tl.label,tim.value])) as TP, tim_ref.decode as tim_ref
      WITH test_code, si,domain, epoch,e_order[0] as e_order,var, bc, dc, d, s,apoc.text.join(apoc.coll.remove(keys(TP),apoc.coll.indexOf(keys(TP),'Main Timeline')),',') as timelines, TP, apoc.text.join(vis,',') as visit, tim_ref
      RETURN 
      si.studyIdentifier as STUDYID
      ,domain.name as DOMAIN
      ,s.identifier as USUBJID
      ,test_code as test_code
      ,bc.name as test_label
      ,var.name as variable
      ,d.value as value
      ,e_order as VISITNUM
      ,visit as VISIT
      ,TP['Main Timeline'] as VISITDY
      ,tim_ref as baseline_timing
      ,duration(TP['Main Timeline']) as ord
      ,TP[timelines] as TPT
      ,epoch[0] as  EPOCH 
      ,bc.uri as uuid
      order by DOMAIN, USUBJID, test_code, e_order,ord ,VISIT, TPT
    """
# vs_query = """
#     match(domain:Domain{name:'VS'})-[:VARIABLE_REL]->(var:Variable)-[:IS_A_REL]->(crm:CRMNode)<-[:IS_A_REL]-(bc_prop:BiomedicalConceptProperty), (domain)-[:USING_BC_REL]->(bc:BiomedicalConcept)-[:PROPERTIES_REL]->(bc_prop)<-[:PROPERTIES_REL]-(dc:DataContract)-[:INSTANCES_REL]->(act_inst_main:ScheduledActivityInstance)<-[:RELATIVE_FROM_SCHEDULED_INSTANCE_REL]-(tim:Timing)
#     OPTIONAL MATCH(act_inst_main)-[:ENCOUNTER_REL]->(e:Encounter),(act_inst_main)-[:EPOCH_REL]->(epoch:StudyEpoch)
#     OPTIONAL MATCH(act_inst_main)<-[:INSTANCES_REL]-(tl:ScheduleTimeline)
#     MATCH (domain)<-[:DOMAIN_REL]-(sd:StudyDesign)<-[:STUDY_DESIGNS_REL]-(sv:StudyVersion)-[:STUDY_IDENTIFIERS_REL]->(si:StudyIdentifier)-[:STUDY_IDENTIFIER_SCOPE_REL]->(sis:Organization {name:'Eli Lilly'})
#     MATCH (dc)<-[:FOR_DC_REL]-(d:DataPoint)-[:FOR_SUBJECT_REL]->(s:Subject)
#     MATCH (bc)-[:CODE_REL]->()-[:STANDARD_CODE_REL]-(code:Code)
#     WITH code.decode as test_code, si,domain, collect(epoch.name) as epoch,collect(toInteger(split(e.id,'_')[1])) as e_order,var, bc, dc, d, s, collect(e.label) as vis, apoc.map.fromPairs(collect([tl.label,tim.value])) as TP, collect(tim.value) as visdy
#     WITH test_code, si,domain, epoch,e_order[0] as e_order,var, bc, dc, d, s,apoc.text.join(apoc.coll.remove(keys(TP),apoc.coll.indexOf(keys(TP),'Main Timeline')),',') as timelines, TP, apoc.text.join(vis,',') as visit, apoc.text.join(visdy,',') as visitdy
#     RETURN 
#     si.studyIdentifier as STUDYID
#     ,domain.name as DOMAIN
#     ,s.identifier as USUBJID
#     ,test_code as test_code
#     ,bc.name as TEST
#     ,var.name as variable
#     ,d.value as value
#     ,e_order as VISITNUM
#     ,visit as VISIT
#     ,visitdy as VISITDY
#     ,duration(TP['Main Timeline']) as ord
#     ,TP[timelines] as TPT
#     ,epoch[0] as  EPOCH 
#     ,bc.uri as uuid
#     order by DOMAIN, USUBJID, test_code, e_order,ord ,VISIT, TPT
#     """


def add_to_debug(results):
    if results:
        for result in results:
            debug.append(result)
    else:
        debug.append("No results from local")
        print("No results from local")

def _query_study_service(query):
    NEO4J_CONNECTION_URI="bolt://localhost:7687"
    NEO4J_USERNAME="neo4j"
    NEO4J_PASSWORD="adminadmin"
    NEO4J_DB="study-service-dev"

    # Driver instantiation
    driver = GraphDatabase.driver(NEO4J_CONNECTION_URI,auth=(NEO4J_USERNAME, NEO4J_PASSWORD))

    # session = self._driver.session(database=self._db_name)
    # response = list(session.run(query))

    with driver.session(database=NEO4J_DB) as session:
        results = session.run(query).data()
        # response = session.run(query)
        # results = [result.data() for result in response]

    driver.close()
    
    # return [result.data() for result in results]
    return results

def query_study_service(query):
    print("Connecting to local Neo4j...",end="")
    if db_is_down():
        print("is not running")
        exit()
    print("connected")
    results = _query_study_service(query)
    l = []
    # for result in results:
    #     debug.append(result)

    return results

def get_vs_input():
    VS_DATA = Path.cwd() / "data" / "input" / "vs.json"
    assert VS_DATA.exists(), "VS_DATA not found"
    with open(VS_DATA) as f:
        vs_data = json.load(f)
    return vs_data

def transpose_findings(domain,input):
    data = {}
    topic = domain+"TESTCD"
    topic_label = domain+"TEST"
    seq_var = domain+"SEQ"
    # sorted_by = sorted(input,key= lambda row: (row['USUBJID'], row['test_code'], row['VISIT'], row['TPT']))
    # keys = [(row['USUBJID'], row['test_code'], row['VISIT'], row['TPT']) for row in input]
    keys = [{'USUBJID':row['USUBJID'],'test_code':row['test_code'], 'VISIT':row['VISIT'], 'TPT':row['TPT']} for row in input]
    keys = [dict(t) for t in {tuple(d.items()) for d in keys}]
    sorted_by = sorted(keys,key= lambda row: (row['USUBJID'], row['test_code'], row['VISIT'], row['TPT']))
    for key in keys:
        print("\n1 key",key)
        debug.append(key)
        rows = [row for row in input if key['USUBJID'] == row['USUBJID'] and key['test_code'] == row['test_code'] and key['VISIT'] == row['VISIT'] and key['TPT'] == row['TPT']]
        for row in rows:
            print(row)
            debug.append(row)
        debug.append(" ")
        # if 'TPT' in row.keys():
        #     key = f"{row['USUBJID']}{row['test_code']}{row['VISIT']}{row['TPT']}"
        # else:
        #     key = f"{row['USUBJID']}{row['test_code']}{row['VISIT']}"
        # if key in data:
        #     print("Add only qualifiers")
        # else:
        #     print("Add first qualifiers")
            

        # data.append(row)

    write_tmp("query-neo4j-local-study-service.txt",debug)

    return data



def compare():
    neo_vs_raw = query_study_service(vs_query)
    neo_vs_raw = neo_vs_raw[0:20]
    for x in neo_vs_raw[0:1]:
        print("x",x)
    neo_vs = transpose_findings("VS",neo_vs_raw)
    source_vs = get_vs_input()
    if len(neo_vs_raw) != len(source_vs):
        print(f"Unequal length source: {len(source_vs)} neo: {len(neo_vs_raw)}")
    else:
        print("VS equal length", len(source_vs))
        

if __name__ == "__main__":
    # query_study_service()
    compare()
