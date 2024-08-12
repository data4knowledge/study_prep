from pathlib import Path
from d4kms_service import Neo4jConnection


def clear_created_nodes():
    db = Neo4jConnection()
    with db.session() as session:
        query = "match (n:Datapoint|DataPoint) detach delete n return count(n)"
        results = session.run(query)
        print("Removing Datapoint/DataPoint",results)
        query = "match (n:Subject) detach delete n return count(n)"
        results = session.run(query)
        print("Removing Subject",results)

def get_import_directory():
    db = Neo4jConnection()
    with db.session() as session:
        query = "call dbms.listConfig()"
        results = session.run(query)
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

def add_identifiers():
    db = Neo4jConnection()
    with db.session() as session:
        query = """
            LOAD CSV WITH HEADERS FROM 'file:///enrolment.csv' AS site_row
            MATCH (design:StudyDesign {name:'Study Design 1'})
            MERGE (s:Subject {identifier:site_row['USUBJID']})
            MERGE (site:StudySite {name:site_row['SITEID']})
            MERGE (s)-[:ENROLLED_AT_SITE_REL]->(site)
            MERGE (site)<-[:MANAGES_SITE]-(researchOrg)
            MERGE (researchOrg)<-[:ORGANIZATIONS_REL]-(design)
            RETURN count(*) as count
        """
        results = session.run(query)
    print("results identifiers/enrolment",results)

def add_datapoints():
    db = Neo4jConnection()
    with db.session() as session:
        query = """
            LOAD CSV WITH HEADERS FROM 'file:///datapoints.csv' AS data_row
            MATCH (dc:DataContract {uri:data_row['DC_URI']})
            MATCH (design:StudyDesign {name:'Study Design 1'})
            MERGE (d:DataPoint {uri: data_row['DATAPOINT_URI'], value: data_row['VALUE']})
            MERGE (s:Subject {identifier:data_row['USUBJID']})
            MERGE (dc)<-[:FOR_DC_REL]-(d)
            MERGE (d)-[:FOR_SUBJECT_REL]->(s)
            RETURN count(*) as count
        """
        results = session.run(query)
    print("results datapoints",results)

def add_identifiers_datapoints():
    db = Neo4jConnection()
    with db.session() as session:
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
        results = session.run(query)
    print("results datapoints",results)

def check_data_contracts():
    db = Neo4jConnection()
    with db.session() as session:
        query = """
            LOAD CSV WITH HEADERS FROM 'file:///datapoints.csv' AS data_row
            RETURN distinct data_row['DC_URI'] as data_contract
        """
        results = session.run(query)
        items = [result.data() for result in results]
    with db.session() as session:
        for item in items:
            query = f"""
                MATCH (dc:DataContract {{uri:'{item['data_contract']}'}})
                WITH COUNT(dc) > 0  as dc_exists
                RETURN dc_exists as exist
            """
            print(query)
            results = session.run(query)
            if results[0].data()['exist']:
                pass
            else:
                print("\n---\ndata_contract MISSING :",item['data_contract'])

def load_datapoints():
    clear_created_nodes()
    import_directory = get_import_directory()
    copy_files_to_db_import(import_directory)
    add_identifiers()
    add_datapoints()
    # check_data_contracts()
    # add_identifiers_datapoints()

if __name__ == "__main__":
    load_datapoints()