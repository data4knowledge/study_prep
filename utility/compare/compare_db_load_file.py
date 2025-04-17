import json
import pandas as pd
from pathlib import Path
from neo4j import GraphDatabase
from d4kms_service import Neo4jConnection, ServiceEnvironment
from utility.neo_utils import db_is_down
from utility.mappings import DATA_LABELS_TO_BC_LABELS, DATA_VISITS_TO_ENCOUNTER_LABELS, DATA_TPT_TO_TIMING_LABELS
from utility.debug import write_debug, write_tmp
import csv

debug = []

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

        #   LOAD CSV WITH HEADERS FROM '{file_path}' AS data_row
        #   MATCH (dc:DataContract {{uri:data_row['DC_URI']}})
        #   MATCH (design:StudyDesign {{name:'Study Design 1'}})
        #   MERGE (d:DataPoint {{uri: data_row['DATAPOINT_URI'], value: data_row['VALUE']}})
        #   MERGE (record:Record {{key:data_row['RECORD_KEY']}})
        #   MERGE (s:Subject {{identifier:data_row['SUBJID']}})
        #   MERGE (dc)<-[:FOR_DC_REL]-(d)
        #   MERGE (d)-[:FOR_SUBJECT_REL]->(s)
        #   MERGE (d)-[:SOURCE]->(record)
        #   RETURN count(*) as count


def query_study_service():
    print("Connecting to local Neo4j...",end="")
    if db_is_down():
        print("is not running")
        exit()
    print("connected")

    # query = """
    #     MATCH (bc:BiomedicalConcept {name:"Sex"})-[:PROPERTIES_REL]->(bcp)
    #     RETURN bcp.uuid as uuid, bcp.name as name, bcp.label as label
    # """
    # results = _query_study_service(query)
    query = """
        MATCH (bc:BiomedicalConcept {name:"Sex"})-[:PROPERTIES_REL]->(bcp)
        RETURN bcp.uuid as uuid, bcp.name as name, bcp.label as label
    """
    results = _query_study_service(query)

    for result in results:
        debug.append(result)

def query_study_service_with_query(query):
    print("Connecting to local Neo4j...",end="")
    if db_is_down():
        print("is not running")
        exit()
    print("connected")
    response = _query_study_service(query)
    results = []
    for result in response:
        results.append(result)
    return results

def get_raw_data():
    filename = "/Users/johannes/Library/CloudStorage/OneDrive-data4knowledge/shared_mac/usdm/data/raw_data_msg.csv"

    df = pd.read_csv(filename)
    return df.to_dict('records')

def compare():
    # query = """
    #     MATCH (dc:DataContract)
    #     RETURN dc.uri as uri
    # """
    # dcs = query_study_service_with_query(query)
    # dcs = [dc['uri'] for dc in dcs]
    query = """
        MATCH (bc:BiomedicalConcept)
        RETURN bc.name as name
    """
    bcs = query_study_service_with_query(query)
    bcs = [bc['name'] for bc in bcs]
    for x in bcs:
        debug.append(x)

    debug.append("--")

    raw_data = get_raw_data()
    raw_bc = list(set([row['LABEL'] for row in raw_data]))
    # for x in raw_bc:
    #     debug.append(x)
    for x in raw_data:
        debug.append(x)
    # raw_data = [row for row in raw_data if row['DC_URI'] in dcs]
    
    write_tmp("query-neo4j-local-study-service.txt",debug)

if __name__ == "__main__":
    # query_study_service()
    compare()
