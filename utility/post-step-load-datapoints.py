from pathlib import Path
from d4kms_service import Neo4jConnection


print("\033[H\033[J") # Clears terminal window in vs code

def clear_created_nodes():
    db = Neo4jConnection()
    query = "match (n:Datapoint|DataPoint) detach delete n return count(n)"
    results = db.query(query)
    print("Removing Datapoint/DataPoint",results)
    query = "match (n:Subject) detach delete n return count(n)"
    results = db.query(query)
    print("Removing Subject",results)
    # query = "match (n:StudySite) detach delete n return count(n)"
    # results = db.query(query)
    # print("Removing StudySite",results)
    db.close()

def get_import_directory():
    db = Neo4jConnection()
    query = "call dbms.listConfig()"
    results = db.query(query)
    db.close()
    config = [x.data() for x in results]
    import_directory = next((item for item in config if item["name"] == 'server.directories.import'), None)
    return import_directory['value']


def copy_file_to_db_import(source, import_directory):
    target_folder = Path(import_directory)
    assert target_folder.exists(), f"Change Neo4j db import directory: {target_folder}"
    target_file = target_folder / source.name
    with open(source,'r') as f:
        txt = f.read()
    with open(target_file,'w') as f:
        f.write(txt)
    print("Written",target_file)

def copy_files_to_db_import(import_directory):
    enrolment_file = Path.cwd() / "data" / "output" / "enrolment.csv"
    assert enrolment_file.exists(), f"enrolment_file does not exist: {enrolment_file}"
    copy_file_to_db_import(enrolment_file, import_directory)
    datapoints_file = Path.cwd() / "data" / "output" / "datapoints.csv"
    assert datapoints_file.exists(), f"datapoints_file does not exist: {datapoints_file}"
    copy_file_to_db_import(datapoints_file, import_directory)

def add_datapoints():
    db = Neo4jConnection()
    query = """
        LOAD CSV WITH HEADERS FROM 'file:///datapoints.csv'  AS data_row
        WITH data_row
        LOAD CSV WITH HEADERS FROM 'file:///enrolment.csv'  AS site_row 
        with data_row, site_row
        MATCH (dc:DataContract {uri:data_row['DC_URI']})
        MATCH (design:StudyDesign {name:'Study Design 1'})-[:ORGANIZATIONS_REL]->(researchOrg)
        MERGE (d:DataPoint {uri: data_row['DATAPOINT_URI'], value: data_row['VALUE']})
        MERGE (s:Subject {identifier:data_row['USUBJID']})
        MERGE (site:StudySite {name:site_row['SITEID']})
        MERGE (dc)<-[:FOR_DC_REL]-(d)
        MERGE (d)-[:FOR_SUBJECT_REL]->(s)
        MERGE (s)-[:ENROLLED_AT_SITE_REL]->(site)
        MERGE (site)<-[:MANAGES_REL]-(researchOrg)
        RETURN count(*)
    """
    results = db.query(query)
    db.close()
    print("results datapoints",results)


clear_created_nodes()
import_directory = get_import_directory()
copy_files_to_db_import(import_directory)
add_datapoints()
