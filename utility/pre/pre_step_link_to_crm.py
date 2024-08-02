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

def clean(txt: str):
    txt = txt.replace(" ","")
    txt = txt.replace(" ","")
    return txt

def make_fake_uuid(name):
    return 'FAKE_UUID'+clean(name)

def clear_created_nodes(db):
    with db.session() as session:
        query = "match (n {fake_node: 'yes' }) detach delete n return count(n) as count"
        results = session.run(query)
        print("Removed fake nodes:",[result.data() for result in results][0]['count'])
        query = "match (n)-[r {fake_relationship: 'yes'}]-(m) delete r return count(r) as count"
        results = session.run(query)
        print("Removed fake relationships:",[result.data() for result in results][0]['count'])

def link_sdtm_variable_to_crm(db,bcp_name,sdtm_variable_name):
    uuid = 0
    with db.session() as session:
        query = f"""
            MATCH (bcp:BiomedicalConceptProperty {{name:"{bcp_name}"}})-[:IS_A_REL]->(crm:CRMNode)
            MATCH (v:Variable {{name:"{sdtm_variable_name}"}})
            with crm, v
            MERGE (v)-[r:IS_A_REL]->(crm)
            set r.fake_relationship = "yes"
            return *
        """
        print(query)
        results = session.run(query)

if __name__ == "__main__":
    db = Neo4jConnection()
    # link_sdtm_variable_to_crm(db, bcp_name="Date of Birth", sdtm_variable_name="BRTHDTC")
    link_sdtm_variable_to_crm(db, bcp_name="DSSTDTC", sdtm_variable_name="DSSTDTC")
