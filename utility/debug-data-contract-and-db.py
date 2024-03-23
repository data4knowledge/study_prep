import json
from pathlib import Path
from debug import write_debug
from d4kms_service import Neo4jConnection

print("\033[H\033[J") # Clears terminal window in vs code

DATA_CONTRACTS_LOOKUP = Path.cwd() / "data" / "output" / "data_contracts.json"
assert DATA_CONTRACTS_LOOKUP.exists(), "DATA_CONTRACTS_LOOKUP not found"
print("\nGetting data contracts from file",DATA_CONTRACTS_LOOKUP)
with open(DATA_CONTRACTS_LOOKUP) as f:
    data_contracts = json.load(f)

def get_all_bc_labels(db):
    query = f"""
    MATCH (bc:BiomedicalConcept)
    return bc.label
    """
    results = db.query(query)
    if results == None:
        print("Not found in neo4j")
    else:
        print("Found in Neo4j")
        return [result.data() for result in results]

def get_all_bcs_and_properties(db):
    query = f"""
    MATCH (bc:BiomedicalConcept)-[:PROPERTIES_REL]->(bcp:BiomedicalConceptProperty)
    return bc.name, bc.label, bcp.name, bcp.label
    """
    results = db.query(query)
    if results == None:
        print("Not found in neo4j")
    else:
        print("Found in Neo4j")
        return [result.data() for result in results]


def get_bc_with_label(db,label):
    query = f"""
    MATCH (bc:BiomedicalConcept)
    WHERE bc.label = '{label}'
    return bc.label
    """
    results = db.query(query)
    if results == None:
        print("Not found in neo4j")
    else:
        print("Found in Neo4j")
        return [result.data() for result in results]
        # for x in [result.data() for result in results]:
        #     print(x)

def get_bc_properties_with_label(db,label):
    query = f"""
    MATCH (bc:BiomedicalConcept)-[:PROPERTIES_REL]->(bcp:BiomedicalConceptProperty)
    WHERE bc.label = '{label}'
    return bcp
    """
    # return bcp
    results = db.query(query)
    if results == None:
        print("Not found in neo4j")
        return []
    else:
        print("Found in Neo4j")
        return [result.data() for result in results]
        # for x in [result.data() for result in results]:
        #     print(x['bcp']['name'],x['bcp']['isEnabled'],x['bcp']['isEnabled'].__class__)
        #     # print(x['bcp'].keys())

# No dc RESULT bc_label: Albumin Measurement - property: Clinical Test Result - encounter: Screening 1
# results = [x for x in data_contracts if x['BC_LABEL'] == "Albumin Measurement" and x['BCP_NAME'] == "Clinical Test Result"]

# get_data_contract Miss BC_LABEL: Alkaline Phosphatase BCP_LABEL: Laboratory Test Result ENCOUNTER_LABEL: Screening 1 TPT: 

label = "Alkaline Phosphatase Measurement"

db = Neo4jConnection()
# db_result = get_all_bc_labels(db)
db_result = get_all_bcs_and_properties(db)
# db_result = get_bc_with_label(db,label)
# db_result = get_bc_properties_with_label(db,label)
db.close()

# db
print("hits db",len(db_result))

write_debug(db_result)
# sub = [x['bcp']['label'] for x in db_result]
# write_debug(sub)

# data contracts
# results = [x for x in data_contracts if x['BC_LABEL'] == label]
results = [tuple([x['BC_LABEL'],x['BCP_NAME']]) for x in data_contracts if x['BC_LABEL'] == label]
print("hits data contracts",len(results))
# write_debug(results)
# write_debug(set(results))
