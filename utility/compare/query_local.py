import json
# import pandas as pd
from pathlib import Path
from neo4j import GraphDatabase
from d4kms_service import Neo4jConnection, ServiceEnvironment
from utility.neo_utils import db_is_down
from utility.mappings import DATA_LABELS_TO_BC_LABELS, DATA_VISITS_TO_ENCOUNTER_LABELS, DATA_TPT_TO_TIMING_LABELS
from utility.debug import write_debug, write_tmp

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

def query_study_service():
    print("Connecting to local Neo4j...",end="")
    if db_is_down():
        print("is not running")
        exit()
    print("connected")

    query = """
    match (n {name: "Date of Birth"})
    return *
    """
    query = """
          MATCH (bc:BiomedicalConcept {name:"Race"})-[:PROPERTIES_REL]->(bcp:BiomedicalConceptProperty {name: '--DTC'})
          RETURN bcp.uuid as uuid, bcp.name as name, bcp.label as label
    """
    query = """
          MATCH (bc:BiomedicalConcept {name:"Race"})-[:PROPERTIES_REL]->(bcp:BiomedicalConceptProperty {name: 'Race'})
          RETURN bcp.uuid as uuid, bcp.name as name, bcp.label as label
    """
    query = """
        MATCH (bc:BiomedicalConcept {name:"Race"})-[:PROPERTIES_REL]->(bcp)
        RETURN bcp.uuid as uuid, bcp.name as name, bcp.label as label
    """
    results = _query_study_service(query)
    l = []
    for result in results:
        debug.append(result)


    write_tmp("query-neo4j-local-study-service.txt",debug)


if __name__ == "__main__":
    query_study_service()
