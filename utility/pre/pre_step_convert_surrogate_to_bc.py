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
    query = "match (n {fake_node: 'yes' }) detach delete n return count(n) as count"
    results = db.query(query)
    print("Removed fake nodes:",[result.data() for result in results][0]['count'])
    query = "match (n)-[r {fake_relationship: 'yes'}]-(m) delete r return count(r) as count"
    results = db.query(query)
    print("Removed fake relationships:",[result.data() for result in results][0]['count'])


def create_bc_from_surrogate(db, new_bc_name):
    # // Create Date of Birth BC
    fake_uuid = make_fake_uuid(new_bc_name)
    query = f"""
        MATCH (bcs:BiomedicalConceptSurrogate)
        where bcs.name = "{new_bc_name}"
        WITH bcs
        MERGE (bc:BiomedicalConcept {{uuid:'{fake_uuid}'}})
        SET bc.name         = bcs.name
        SET bc.description  = bcs.description
        SET bc.label        = bcs.label
        SET bc.name         = bcs.name
        SET bc.instanceType = "BiomedicalConcept"
        SET bc.reference    = "None set"
        SET bc.id           = "BiomedicalConcept_999"
        SET bc.fake_node    = "yes"
        return bc.uuid
    """
    results = db.query(query)
    return fake_uuid
    return [result.data() for result in results]

def copy_bc_properties_from_bc(db, new_bc_name,copy_bc_name, bc_uuid):
    # Get properties for bc to copy
    query = f"""
        MATCH (bc:BiomedicalConcept {{name:"{copy_bc_name}"}})-[:PROPERTIES_REL]->(bcp)
        RETURN bcp.uuid as uuid, bcp.name as name, bcp.label as label
    """
    # print("query\n",query)
    # Create the same properties for new bc and add relationships to data contract and scheduled activity instance
    results = db.query(query)
    for bcp in [result.data() for result in results]:
        uuid = str(uuid4())
        # Create property nodes
        bcp_name = bcp['name'] if bcp['name'] != copy_bc_name else new_bc_name
        bcp_label = bcp['label'] if bcp['label'] != copy_bc_name else new_bc_name
        # HARD CODING
        if bcp_label == 'Date of Birth':
            bcp_label = 'Date/Time of Birth'
        print('bcp_label',bcp_label,new_bc_name)
        # print('bcp_name',bcp_name)
        query = f"""
            MATCH (source_bcp:BiomedicalConceptProperty {{uuid:'{bcp['uuid']}'}})
            with source_bcp
            MERGE (bcp:BiomedicalConceptProperty {{uuid:'{uuid}'}})
            SET bcp.datatype     =	source_bcp.datatype
            SET bcp.id           =	'tbd'
            SET bcp.instanceType =	source_bcp.instanceType
            SET bcp.isEnabled    =	source_bcp.isEnabled
            SET bcp.isRequired   =	source_bcp.isRequired
            SET bcp.label        =	'{bcp_label}'
            SET bcp.name         =	'{bcp_name}'
            SET bcp.fake_node    = 'yes'
            RETURN bcp.uuid as uuid
        """
        # print(query)
        results = db.query(query)
        # Link to new bc
        query = f"""
            MATCH (bc:BiomedicalConcept {{uuid:'{make_fake_uuid(new_bc_name)}'}})
            MATCH (bcp:BiomedicalConceptProperty {{uuid:'{uuid}'}})
            MERGE (bc)-[r:PROPERTIES_REL]->(bcp)
            set r.fake_relationship = 'yes'
        """
        results = db.query(query)
        # Copy property nodes relationships: DataContract
        dc_uri="https://study.d4k.dk/study-cdisc-pilot-lzzt/"+bc_uuid+"/"+uuid
        query = f"""
            MATCH (source_bcp:BiomedicalConceptProperty {{uuid:'{bcp['uuid']}'}})<-[:PROPERTIES_REL]-(dc:DataContract)-[:INSTANCES_REL]-(sai:ScheduledActivityInstance)
            MATCH (bcp:BiomedicalConceptProperty {{uuid:'{uuid}'}})
            with sai, bcp
            CREATE (dc:DataContract {{uri:'{dc_uri}', fake_node: 'yes'}})
            CREATE (bcp)<-[r1:PROPERTIES_REL]-(dc)-[r2:INSTANCES_REL]->(sai)
            set r1.fake_relationship = 'yes'
            set r2.fake_relationship = 'yes'
            RETURN 'done'
        """
        results = db.query(query)
        # Copy property nodes relationships: IS_A_REL        
        query = f"""
            MATCH (source_bcp:BiomedicalConceptProperty {{uuid:"{bcp['uuid']}"}})-[:IS_A_REL]->(crm:CRMNode)
            MATCH (bcp:BiomedicalConceptProperty {{uuid:"{uuid}"}})
            with crm, bcp
            MERGE (bcp)-[r:IS_A_REL]->(crm)
            set r.fake_relationship = "yes"
            return *
        """
        # print(query)
        results = db.query(query)


def link_birthdtc_to_crm(db):
    # If topic result (e.g. Date of Birth)
    # if bcp['name'] != copy_bc_name:
    bcp_name = "Date of Birth"
    uuid = 0
        # MATCH (source_bcp:BiomedicalConceptProperty {{name:"{bcp_name}"}})-[:IS_A_REL]->(crm:CRMNode)
    query = f"""
        MATCH (bcp:BiomedicalConceptProperty {{name:"{bcp_name}"}})-[:IS_A_REL]->(crm:CRMNode)
        MATCH (v:Variable {{name:"BRTHDTC"}})
        with crm, v
        MERGE (v)-[r:IS_A_REL]->(crm)
        set r.fake_relationship = "yes"
        return *
    """
    print(query)
    results = db.query(query)


def copy_bc_relationships_from_bc(db, new_bc_name,copy_bc_name):
    # Copy relationship to study
    query = f"""
        MATCH (copy_bc:BiomedicalConcept {{name:"{copy_bc_name}"}})<-[:BIOMEDICAL_CONCEPTS_REL]-(target:StudyDesign)
        MATCH (new_bc:BiomedicalConcept {{uuid:"{make_fake_uuid(new_bc_name)}"}})
        MERGE (new_bc)<-[:BIOMEDICAL_CONCEPTS_REL]-(target)
    """
    # print(query)
    results = db.query(query)
    # Copy relationship to domain
    query = f"""
        MATCH (copy_bc:BiomedicalConcept {{name:"{copy_bc_name}"}})<-[:USING_BC_REL]-(target:Domain)
        MATCH (new_bc:BiomedicalConcept {{uuid:"{make_fake_uuid(new_bc_name)}"}})
        MERGE (new_bc)<-[:USING_BC_REL]-(target)
    """
    # print(query)
    results = db.query(query)
    # Copy relationship to activity
    query = f"""
        MATCH (copy_bc:BiomedicalConcept {{name:"{copy_bc_name}"}})<-[:BIOMEDICAL_CONCEPT_REL]-(target:Activity)
        MATCH (new_bc:BiomedicalConcept {{uuid:"{make_fake_uuid(new_bc_name)}"}})
        MERGE (new_bc)<-[:BIOMEDICAL_CONCEPT_REL]-(target)
    """
    # print(query)
    results = db.query(query)
    # return [result.data() for result in results]

    # Ignoring CODE_REL -> AliasCode


def create_bc_from_existing_surrogate(db, new_bc_name, copy_bc):
    bc_uuid = create_bc_from_surrogate(db, new_bc_name)
    # Copy properties from another BC
    bcp_uuids = copy_bc_properties_from_bc(db, new_bc_name, copy_bc, bc_uuid)
    copy_bc_relationships_from_bc(db, new_bc_name, copy_bc)
    link_birthdtc_to_crm(db)
    print("bcp_uuids",bcp_uuids)


# def create_bc_properties_from_bc(db, name):

    # print(query)
    # results = db.query(query)
    # return [result.data() for result in results]

def convert_surrogate_to_bc():
    db = Neo4jConnection()

    # Add vs data to the graph
    print("Starting")
    all_data = []

    clear_created_nodes(db)
    create_bc_from_existing_surrogate(db,'Date of Birth','Race')
    # timelines = get_main_timeline_with_sub_timeline(db)
    # for result in timelines:
    #     print("----")
    #     create_data_contracts(db, result['sub_timeline_uuid'], result['main_timeline_sai_uuids'])

    db.close()

    write_debug("debug-convert_surrogate.txt",debug)


if __name__ == "__main__":
    convert_surrogate_to_bc()
