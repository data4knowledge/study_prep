import os
import csv
from pathlib import Path
from d4kms_service import Neo4jConnection


print("\033[H\033[J") # Clears terminal window in vs code

def clear_created_nodes(db):
    query = "match (n:Datapoint) detach delete n return count(n)"
    results = db.query(query)
    # print("results1",results)
    query = 'match (n:DataContract {delete:"me"}) detach delete n return count(n)'
    results = db.query(query)
    # print("results2",results)
    query = 'match ()-[r:DC_TO_MAIN_TIMELINE]-() detach delete r return count(r)'
    results = db.query(query)
    # print("results4",results)


def get_main_timeline_with_sub_timeline(db):
    query = """
        MATCH (act:Activity)-[:TIMELINE_REL]->(sub_timeline:ScheduleTimeline)
        MATCH (act)<-[:ACTIVITY_REL]-(main_sai:ScheduledActivityInstance)
        MATCH (main_sai)-[:ENCOUNTER_REL]->(enc:Encounter)
        return  sub_timeline.name AS sub_timeline_name,sub_timeline.uuid as sub_timeline_uuid, collect(main_sai.uuid) as main_timeline_sai_uuids
    """
    # print('get query',query)
    results = db.query(query)
    return [result.data() for result in results]

def create_data_contracts(db, sub_timeline_uuid, main_timeline_sai_uuids):
    num_nodes = 0
    # for main_sai_uuid in main_timeline_sai_uuids[0:2]:
    for main_sai_uuid in main_timeline_sai_uuids:
        # uri: https://study.d4k.dk/study-cdisc-pilot-lzzt/75fded6a-96fe-414d-afcc-5e16438b25d7/09ccd782-231a-4156-97df-8ff8fbfce1c8
        # main acticity uuid + timeline activity uuid + bc property id
        uri_root = 'https://study.d4k.dk/study-cdisc-pilot-lzzt'+'/'+main_sai_uuid+'/'
        query = """
            MATCH (main_t:ScheduledActivityInstance {uuid:'%s'})
            MATCH (stl:ScheduleTimeline {uuid:'%s'})-[:INSTANCES_REL]->(sai:ScheduledActivityInstance)
            MATCH (sai)-[:ACTIVITY_REL]->(o_a:Activity)-[:BIOMEDICAL_CONCEPT_REL]->(bc:BiomedicalConcept)-[:PROPERTIES_REL]->(bcp:BiomedicalConceptProperty)
            MATCH (sai)<-[:RELATIVE_FROM_SCHEDULED_INSTANCE_REL]-(t:Timing)
            with main_t, sai, t, bcp
            CREATE (dc:DataContract {delete:'me'})
            MERGE (dc)-[:DC_TO_MAIN_TIMELINE]->(main_t)
            MERGE (dc)-[:INSTANCES_REL]->(sai)
            MERGE (dc)-[:PROPERTIES_REL]->(bcp)
            SET dc.uri = '%s' + sai.uuid + '/' + bcp.uuid
            return  *
        """ % (main_sai_uuid, sub_timeline_uuid, uri_root)

        # print('create query',query)
        results = db.query(query, sub_timeline_uuid)
        if results == None or results == []:
            print("Query probably didn't work")
            print("uri",uri)
            print("query",query)
            print("---")
        else:
            # print("Query results",len(results))
            num_nodes = num_nodes + len(results)
        # print('results',results)
        # return [result.data() for result in results]
    # return [result.data() for result in results]
    print("added nodes",num_nodes)
    return True

def get_start(db):
    # query = f"""
    #     MATCH (act:Activity)-[:TIMELINE_REL]->(other_stl:ScheduleTimeline {name:'Vital Sign Blood Pressure Timeline'})
    #     MATCH (act)<-[:ACTIVITY_REL]-(main_sai:ScheduledActivityInstance)
    #     MATCH (main_sai)-[:ENCOUNTER_REL]->(enc:Encounter)-[:SCHEDULED_AT_REL]->(main_t:Timing)
    #     MATCH (other_stl)-[:INSTANCES_REL]->(other_sai:ScheduledActivityInstance)
    #     MATCH (other_sai)-[:ACTIVITY_REL]->(o_a:Activity)-[:BIOMEDICAL_CONCEPT_REL]->(bc:BiomedicalConcept)-[:PROPERTIES_REL]->(bcp:BiomedicalConceptProperty)
    #     MATCH (other_sai)<-[:RELATIVE_FROM_SCHEDULED_INSTANCE_REL]-(o_t:Timing)
    #     //return  act, main_sai, main_t, enc, other_stl, other_sai, o_a, bc, bcp, o_t
    #     //WITH main_t, other_sai, bcp, o_t
    #     WITH main_t, o_t, other_sai, bcp
    #     //UNWIND 
    #     return *
    # """
    query = """
        MATCH (act:Activity)-[:TIMELINE_REL]->(other_stl:ScheduleTimeline {name:'%s'})
        MATCH (act)<-[:ACTIVITY_REL]-(main_sai:ScheduledActivityInstance)
        MATCH (main_sai)-[:ENCOUNTER_REL]->(enc:Encounter)-[:SCHEDULED_AT_REL]->(main_t:Timing)
        MATCH (other_stl)-[:INSTANCES_REL]->(other_sai:ScheduledActivityInstance)
        MATCH (other_sai)-[:ACTIVITY_REL]->(o_a:Activity)-[:BIOMEDICAL_CONCEPT_REL]->(bc:BiomedicalConcept)-[:PROPERTIES_REL]->(bcp:BiomedicalConceptProperty)
        MATCH (other_sai)<-[:RELATIVE_FROM_SCHEDULED_INSTANCE_REL]-(o_t:Timing)
        //return  act, main_sai, main_t, enc, other_stl, other_sai, o_a, bc, bcp, o_t
        //WITH main_t, other_sai, bcp, o_t
        WITH main_t, o_t, other_sai, bcp
        //UNWIND 
        return *
    """ % ('Vital Sign Blood Pressure Timeline')
    print('bcp query',query)
    results = db.query(query)
    # print('results',results)
    # if results == None:
    #     print("DataContract has errors in it",row['VISIT'],visit,bc_label,query)
    #     return []
    # if results == []:
    #     print("DataContract query did not yield any results",row['VISIT'],visit,bc_label)
    #     return []
    # # print("contract query alright")
    return [result.data() for result in results]


db = Neo4jConnection()

# Add vs data to the graph
# add_vs(db, vs_data, subjects)
print("Starting")
all_data = []

clear_created_nodes(db)
timelines = get_main_timeline_with_sub_timeline(db)
for result in timelines:
    print("----")
    # print(result.keys())
    # print(result['sub_timeline_uuid'])
    create_data_contracts(db, result['sub_timeline_uuid'], result['main_timeline_sai_uuids'])


    # print(result.keys())

# results = get_start(db)
# for result in results:
#     print(result.keys())

db.close()
