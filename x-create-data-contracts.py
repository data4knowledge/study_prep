import os
import csv
from pathlib import Path
# from neo4j import GraphDatabase
# from d4kms_generic import ServiceEnvironment
from d4kms_service import Neo4jConnection


print("\033[H\033[J") # Clears terminal window in vs code

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
print("Looping VS")
all_data = []

results = get_start(db)
for result in results:
    print(result.keys())

db.close()
