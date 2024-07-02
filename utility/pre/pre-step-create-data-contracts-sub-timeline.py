import os
import csv
from pathlib import Path
from d4kms_service import Neo4jConnection


# print("\033[H\033[J") # Clears terminal window in vs code

def clear_created_nodes(db):
    query = "match (n:Datapoint|DataPoint) detach delete n return count(n)"
    results = db.query(query)
    print("Removed Datapoint/DataPoint:",results)
    query = 'match (n:DataContract {delete:"me"}) detach delete n return count(n)'
    results = db.query(query)
    print("Removed DataContract:",results)


def get_main_timeline_with_sub_timeline(db):
    query = """
        MATCH (act:Activity)-[:TIMELINE_REL]->(sub_timeline:ScheduleTimeline)
        MATCH (act)<-[:ACTIVITY_REL]-(main_sai:ScheduledActivityInstance)
        MATCH (main_sai)-[:ENCOUNTER_REL]->(enc:Encounter)
        return sub_timeline.name AS sub_timeline_name,sub_timeline.uuid as sub_timeline_uuid, collect(main_sai.uuid) as main_timeline_sai_uuids
    """
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
            MERGE (dc)-[:INSTANCES_REL]->(main_t)
            MERGE (dc)-[:INSTANCES_REL]->(sai)
            MERGE (dc)-[:PROPERTIES_REL]->(bcp)
            SET dc.uri = '%s' + sai.uuid + '/' + bcp.uuid
            return  *
        """ % (main_sai_uuid, sub_timeline_uuid, uri_root)

        # print('create query',query)
        results = db.query(query, sub_timeline_uuid)
        if results == None or results == []:
            print("Query probably didn't work")
            print("uri",uri_root)
            print("query",query)
            print("---")
        else:
            # print("Query results",len(results))
            num_nodes = num_nodes + len(results)
    print("added",num_nodes, "data contract nodes")
    return True

db = Neo4jConnection()

# Add vs data to the graph
print("Starting")
all_data = []

clear_created_nodes(db)
timelines = get_main_timeline_with_sub_timeline(db)
for result in timelines:
    print("----")
    create_data_contracts(db, result['sub_timeline_uuid'], result['main_timeline_sai_uuids'])

db.close()
