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

def get_study_design():
    query = f"""
        MATCH (sd:StudyDesign)
        RETURN sd
    """
    # debug.append(query)
    results = db_query(query)
    return results

debug = []

def check_query():
    query = f"""
        // Find BC's used
        MATCH (sd:StudyDesign {{uuid: '47343f4e-c9e0-44cf-b5de-09c3baf455d5'}})-[:BIOMEDICAL_CONCEPTS_REL]->(bc:BiomedicalConcept)
        WITH distinct bc.name as bc_name
        WITH collect(bc_name) as names
        unwind names as name
        // Get only one match per name
        CALL {{
            WITH name
            MATCH (bc:BiomedicalConcept)
            WHERE bc.name = name
            return bc
            limit 1
        }}
        WITH bc
        MATCH (bc)-[:CODE_REL]-(:AliasCode)-[:STANDARD_CODE_REL]->(cd:Code)
        MATCH (bc)-[:PROPERTIES_REL]->(bcp:BiomedicalConceptProperty)
        MATCH (bcp)-[:IS_A_REL]->(crm:CRMNode)
        OPTIONAL MATCH (d:Domain)-[:USING_BC_REL]->(bc)
        OPTIONAL MATCH (crm)<-[:IS_A_REL]-(var:Variable)<-[:VARIABLE_REL]-(d)
        WHERE NOT EXISTS {{
        (bcp)-[:RESPONSE_CODES_REL]->(:ResponseCode)-[:CODE_REL]->(:Code)
        }}
        return "first" as from, bc.name as bc, cd.decode as bc_name, bcp.name as name, crm.datatype as data_type, collect({{domain:d.name,variable:var.name}}) as sdtm, [] as terms
        union
        MATCH (bc)-[:CODE_REL]-(:AliasCode)-[:STANDARD_CODE_REL]->(cd:Code)
        MATCH (bc)-[:PROPERTIES_REL]->(bcp:BiomedicalConceptProperty)
        MATCH (bcp)-[:IS_A_REL]->(crm:CRMNode)
        MATCH (bcp)-[:RESPONSE_CODES_REL]->(rc:ResponseCode)-[:CODE_REL]->(c:Code)
        OPTIONAL MATCH (d:Domain)-[:USING_BC_REL]->(bc)
        OPTIONAL MATCH (crm)<-[:IS_A_REL]-(var:Variable)<-[:VARIABLE_REL]-(d)
        WITH distinct bc.name as bc, cd.decode as bc_name, bcp.name as name, crm.datatype as data_type, d.name as domain, var.name as variable, c.code as code, c.decode as pref_label, c.decode as notation
        return "second" as from, bc, bc_name, name, data_type, collect({{domain:domain,variable:variable}}) as sdtm, collect({{code:code,pref_label:pref_label,notation:notation}}) as terms
    """
    query = f"""
        MATCH (sd:StudyDesign {{uuid: '47343f4e-c9e0-44cf-b5de-09c3baf455d5'}})-[:BIOMEDICAL_CONCEPTS_REL]->(bc:BiomedicalConcept)
        WITH distinct bc.name as bc_name
        WITH collect(bc_name) as names
        unwind names as name
        // Get only one match per name
        CALL {{
          WITH name
          MATCH (bc:BiomedicalConcept)
          WHERE bc.name = name
          return bc
          limit 1
        }}
        WITH bc
        MATCH (bc)-[:CODE_REL]-(:AliasCode)-[:STANDARD_CODE_REL]->(cd:Code)
        MATCH (bc)-[:PROPERTIES_REL]->(bcp:BiomedicalConceptProperty)
        MATCH (bcp)-[:IS_A_REL]->(crm:CRMNode)
        WHERE NOT EXISTS {{
          (bcp)-[:RESPONSE_CODES_REL]->(:ResponseCode)-[:CODE_REL]->(:Code)
        }}
        WITH bc, bcp, cd, crm
        OPTIONAL MATCH (d:Domain)-[:USING_BC_REL]->(bc)
        OPTIONAL MATCH (crm)<-[:IS_A_REL]-(var:Variable)<-[:VARIABLE_REL]-(d)
        WITH distinct bc.name as bc, cd.decode as bc_name, bcp.name as name, crm.datatype as data_type, d.name as domain, var.name as variable, "" as code, "" as pref_label, "" as notation
        return "first" as from, bc, bc_name, name, data_type, collect({{domain:domain,variable:variable}}) as sdtm, [] as terms
        union
        MATCH (bc)-[:CODE_REL]-(:AliasCode)-[:STANDARD_CODE_REL]->(cd:Code)
        MATCH (bc)-[:PROPERTIES_REL]->(bcp:BiomedicalConceptProperty)
        MATCH (bcp)-[:IS_A_REL]->(crm:CRMNode)
        MATCH (bcp)-[:RESPONSE_CODES_REL]->(rc:ResponseCode)-[:CODE_REL]->(c:Code)
        OPTIONAL MATCH (d:Domain)-[:USING_BC_REL]->(bc)
        OPTIONAL MATCH (crm)<-[:IS_A_REL]-(var:Variable)<-[:VARIABLE_REL]-(d)
        WITH distinct bc.name as bc, cd.decode as bc_name, bcp.name as name, crm.datatype as data_type, d.name as domain, var.name as variable, c.code as code, c.decode as pref_label, c.decode as notation
        return "second" as from, bc, bc_name, name, data_type, collect({{domain:domain,variable:variable}}) as sdtm, collect({{code:code,pref_label:pref_label,notation:notation}}) as terms
        order by bc_name
    """
    query = f"""
   // Find BC's used
        MATCH (sd:StudyDesign {{uuid: '47343f4e-c9e0-44cf-b5de-09c3baf455d5'}})-[:BIOMEDICAL_CONCEPTS_REL]->(bc:BiomedicalConcept)
        WITH distinct bc.name as bc_name
        WITH collect(bc_name) as names
        unwind names as name
        // Get only one match per name
        CALL {{
          WITH name
          MATCH (bc:BiomedicalConcept)
          WHERE bc.name = name
          return bc
          limit 1
        }}
        WITH bc
        MATCH (bc)-[:CODE_REL]-(:AliasCode)-[:STANDARD_CODE_REL]->(cd:Code)
        MATCH (bc)-[:PROPERTIES_REL]->(bcp:BiomedicalConceptProperty)
        MATCH (bcp)-[:IS_A_REL]->(crm:CRMNode)
        WHERE NOT EXISTS {{ 
          (bcp)-[:RESPONSE_CODES_REL]->(:ResponseCode)-[:CODE_REL]->(:Code)
        }}
        WITH bc, bcp, cd, crm
        OPTIONAL MATCH (d:Domain)-[:USING_BC_REL]->(bc)
        OPTIONAL MATCH (crm)<-[:IS_A_REL]-(var:Variable)<-[:VARIABLE_REL]-(d)
        WITH distinct bc.name as bc, cd.decode as bc_name, bcp.name as name, crm.datatype as data_type, d.name as domain, var.name as variable, "" as code, "" as pref_label, "" as notation
        return "no code" as from, bc, bc_name, name, data_type, collect({{domain:domain,variable:variable}}) as sdtm, [] as terms
        union
        MATCH (bc)-[:CODE_REL]-(:AliasCode)-[:STANDARD_CODE_REL]->(cd:Code)
        MATCH (bc)-[:PROPERTIES_REL]->(bcp:BiomedicalConceptProperty)
        MATCH (bcp)-[:IS_A_REL]->(crm:CRMNode)
        MATCH (bcp)-[:RESPONSE_CODES_REL]->(rc:ResponseCode)-[:CODE_REL]->(c:Code)
        OPTIONAL MATCH (d:Domain)-[:USING_BC_REL]->(bc)
        OPTIONAL MATCH (crm)<-[:IS_A_REL]-(var:Variable)<-[:VARIABLE_REL]-(d)
        WITH distinct bc.name as bc, cd.decode as bc_name, bcp.name as name, crm.datatype as data_type, d.name as domain, var.name as variable, c.code as code, c.decode as pref_label, c.decode as notation
        return "coded" as from, bc, bc_name, name, data_type, collect({{domain:domain,variable:variable}}) as sdtm, collect({{code:code,pref_label:pref_label,notation:notation}}) as terms
    """
    # debug.append(query)
    results = db_query(query)
    for x in results:
        debug.append(f"{x}")

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
    sd = get_study_design()
    # debug.append(sd)
    check_query()
    # find_bc()
    write_tmp("check_db.txt", debug)
    pass