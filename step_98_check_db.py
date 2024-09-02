from pathlib import Path
from d4kms_service import Neo4jConnection
from model.configuration import Configuration, ConfigurationNode
from itertools import groupby
from neo4j import GraphDatabase

def write_tmp(name, data):
    TMP_PATH = Path.cwd() / "tmp" / "saved_debug"
    OUTPUT_FILE = TMP_PATH / name
    print("Writing file...",OUTPUT_FILE.name,OUTPUT_FILE, end="")
    with open(OUTPUT_FILE, 'w') as f:
        for it in data:
            f.write(str(it))
            f.write('\n')
    print(" ...done")

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

def get_bc_property_terms():
    query = f"""
        MATCH (bc:BiomedicalConcept)-[:PROPERTIES_REL]->(bcp:BiomedicalConceptProperty)
        MATCH (bcp)-[:IS_A_REL]->(crm:CRMNode)
        MATCH (bcp)-[:RESPONSE_CODES_REL]->(rc:ResponseCode)-[:CODE_REL]->(c:Code)
        WHERE crm.datatype = "coding"
        WITH distinct bc.name as bc, bcp.name as property, c.decode as term
        RETURN bc, property, collect(term) as terms
        UNION
        MATCH (bc:BiomedicalConcept)-[:PROPERTIES_REL]->(bcp:BiomedicalConceptProperty)
        MATCH (bcp)-[:IS_A_REL]->(crm:CRMNode)
        WHERE crm.datatype = "coding"
        AND NOT EXISTS {{
            MATCH (bcp)-[:RESPONSE_CODES_REL]->(rc:ResponseCode)
        }}
        RETURN distinct bc.name as bc, bcp.name as property, "missing" as terms
    """
    results = db_query(query)
    return results

debug = []

def check_coding_terms():
    results = get_bc_property_terms()

    def key_func(k):
        return k['bc']

    for key, values in groupby(results, key_func):
        debug.append(f"{key}")
        for v in values:
            # debug.append(f"{v}")
            debug.append(f"  {v['property']}")
            if v['terms'] != "missing":
                for t in v['terms']:
                    debug.append(f"    {t}")


def get_bc():
    add_terms = {
      'Race': ['Race'],     
      'Aspartate Aminotransferase in Serum/Plasma': ['LBSPEC'],
      'Creatinine Concentration in Urine': ['LBSPEC'],
      'Alkaline Phosphatase Concentration in Serum/Plasma': ['LBSPEC'],
      'Sodium Concentration in Urine': ['LBSPEC'],
      'Potassium Concentration in Urine': ['LBSPEC'],
      'Alanine Aminotransferase Concentration in Serum/Plasma': ['LBSPEC'],
      'Albumin Presence in Urine': ['LBSPEC'],
      'Hemoglobin A1C Concentration in Blood': ['LBSPEC'],
      'Adverse Event Prespecified': ['AELOC'],
      'Exposure Unblinded': ['EXDOSFRQ','EXROUTE','EXTRT','EXDOSFRM'],
      'Adverse Event Prespecified': ['AERELNST','AEREL','AEACNOTH','AEACN','AESEV','AETERM'],
    }

    query = f"""
        MATCH (bc:Instance)-[:HAS_ITEM]->(bcp)
        where bc.name = 'Race'
        RETURN *
    """
    results = query_aura(query)
    return results


def find_bc():
    results = get_bc()
    for x in results:
        debug.append(f"{x}")

if __name__ == "__main__":
    # check_coding_terms()
    find_bc()
    write_tmp("check_db.txt", debug)
    pass