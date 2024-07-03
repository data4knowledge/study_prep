import json
# import pandas as pd
from pathlib import Path
from neo4j import GraphDatabase
from d4kms_service import Neo4jConnection, ServiceEnvironment
from utility.neo_utils import db_is_down
from utility.mappings import DATA_LABELS_TO_BC_LABELS, DATA_VISITS_TO_ENCOUNTER_LABELS, DATA_TPT_TO_TIMING_LABELS
from utility.debug import write_debug, write_tmp

debug = []

def query_aura(query):
    # BC
    AURA_CONNECTION_URI='neo4j+s://f039618b.databases.neo4j.io'
    AURA_USERNAME='neo4j'
    AURA_PASSWORD='HMfxuudpsVxOHPe_yOMELEBGKfygXRtRpHrZhW6VKvw'
    # AURA_INSTANCEID=f039618b
    # AURA_INSTANCENAME=BC Database

    # Driver instantiation
    driver = GraphDatabase.driver(AURA_CONNECTION_URI,auth=(AURA_USERNAME, AURA_PASSWORD))

    with driver.session() as session:
        # Use .data() to access the results array
        results = session.run(query).data()

    driver.close()
    
    return results


def query_local(query):
    NEO4J_CONNECTION_URI="bolt://localhost:7687"
    NEO4J_USERNAME="neo4j"
    NEO4J_PASSWORD="adminadmin"
    NEO4J_DB="bc-service-dev"

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

def add_to_debug(results):
    if results:
        for result in results:
            debug.append(result)
    else:
        debug.append("No results from local")
        print("No results from local")

def query_both(query):
    local_results = query_local(query)
    aura_results = query_aura(query)
    return {"local":local_results,"aura":aura_results}


# ScopedIdentifier
# InstanceItem
# ValueSet
# RegistrationStatus
# Namespace
# DataType
# RegistrationAuthority
# TemplateItem
# DataTypeProperty
# Package
# D4kTemplate
# D4kInstance
# CDISCGenericInstance
# DataElementConcept
# CDISCSdtmInstance
# Variable
# CodeList
# AssignedTerm


    # query = """
    # call db.labels()
    # """

def compare_bc():
    print("Connecting to local Neo4j...",end="")
    if db_is_down():
        print("is not running")
        exit()
    print("connected")

# InstanceItem
# D4kInstance
# CDISCGenericInstance
# CDISCSdtmInstance
    query = """
         match (i:InstanceItem) return i.name as name
    """
    query = """
        MATCH (n:CDISCGenericInstance|CDISCSdtmInstance|D4kInstance)-[:HAS_STATUS]->(s)-[:MANAGED_BY]->(ra:RegistrationAuthority)
        where toLower(n.name) contains "informed"
        RETURN n,s,ra LIMIT 25
    """
    query = """
        MATCH (n:CDISCGenericInstance|CDISCSdtmInstance|D4kInstance)-[:HAS_STATUS]->(s)-[:MANAGED_BY]->(ra:RegistrationAuthority)
        where toLower(n.name) contains "date"
        RETURN n,s,ra LIMIT 25
    """
    results = query_local(query)
    # for x in results:
    #     add_to_debug(f"x.__class__: {x.__class__}")
    add_to_debug([x for x in results])
    # add_to_debug([x['name'] for x in results])
    # print(results)

    
    # query = """
    # call db.labels()
    # """
    # results = query_both(query)
    # l = []
    # # for result in results['local']:
    # #     l.append(result.keys())
    # debug.append("-- local")
    # add_to_debug([x['label'] for x in results['local']])
    # debug.append("-- aura")
    # add_to_debug([x['label'] for x in results['aura']])


    # add_to_debug(results['local'])
    # debug.append("-- aura")
    # add_to_debug(results['aura'])


    write_tmp("neo4j-result-bc.txt",debug)


if __name__ == "__main__":
    compare_bc()
