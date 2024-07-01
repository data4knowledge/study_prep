import os
import csv
from pathlib import Path
from d4kms_service import Neo4jConnection
from uuid import uuid4

# <============= DEBUG
def write_debug(name, data):
    from pathlib import Path
    import os
    TMP_PATH = Path.cwd() / "tmp" / "saved_debug"
    if not os.path.isdir(TMP_PATH):
      os.makedirs(TMP_PATH)
    OUTPUT_FILE = TMP_PATH / name
    print("Writing file...",OUTPUT_FILE.name,OUTPUT_FILE, end="")
    with open(OUTPUT_FILE, 'w') as f:
        for it in data:
            f.write(str(it))
            f.write('\n')
    print(" ...done")

def add_debug(*txts):
    for txt in txts:
        debug.append(str(txt))

debug = []
# =============> DEBUG

def clear_created_nodes(db):
    query = """
    MATCH (n)-[r]->(m)
    WHERE r.fake_crm =  'yes'
    DELETE r
    RETURN count(r) as count
    """
    # print(query)
    results = db.query(query)
    print("Removed fake_crm relationships:",[result.data() for result in results][0]['count'])


def link_dsstdtc_to_dm(db):
    # Copy relationship to study
    query = f"""
        MATCH (crm:CRMNode)
        MATCH (bcp:BiomedicalConceptProperty)
        WHERE crm.uri = 'https://crm.d4k.dk/dataset/observation/observation_result/result/coding/code'
        AND  bcp.name = 'DSSTDTC'
        MERGE (bcp)-[r:IS_A_REL]->(crm)
        SET r.fake_crm = 'yes'
        RETURN *
    """
    # print(query)
    results = db.query(query)

def link_rficdtc_to_crm(db):
    # Copy RFICDTC to CRMnode
    query = f"""
        MATCH (crm:CRMNode)
        MATCH (var:Variable)
        WHERE crm.uri = 'https://crm.d4k.dk/dataset/observation/observation_result/result/coding/code'
        AND  var.name = 'RFICDTC'
        MERGE (var)-[r:IS_A_REL]->(crm)
        SET r.fake_crm = 'yes'
        RETURN *
    """
    print(query)
    results = db.query(query)


def make_links():
    db = Neo4jConnection()
    clear_created_nodes(db)
    link_dsstdtc_to_dm(db)
    link_rficdtc_to_crm(db)
    db.close()

    write_debug("debug-link-informed-consent.txt",debug)

if __name__ == "__main__":
    try:
        make_links()
    except Exception as e:
        print("problems",e)

