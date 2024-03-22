# import os
# import csv
from pathlib import Path
from d4kms_service import Neo4jConnection


print("\033[H\033[J") # Clears terminal window in vs code

def clear_created_nodes(db):
    query = "match (n:Datapoint|DataPoint) detach delete n return count(n)"
    results = db.query(query)
    print("results Datapoint/DataPoint",results)
    query = "match (n:Subject) detach delete n return count(n)"
    results = db.query(query)
    print("results Subject",results)

def add_datapoints(db):
    query = """
        LOAD CSV WITH HEADERS FROM 'file:///datapoints.csv'  AS data_row
        WITH data_row
        LOAD CSV WITH HEADERS FROM 'file:///enrolment.csv'  AS site_row 
        with data_row, site_row
        MATCH (dc:DataContract {uri:data_row['DC_URI']})
        MATCH (design:StudyDesign {name:'Study Design 1'})
        MERGE (d:DataPoint {uri: data_row['DATAPOINT_URI'], value: data_row['VALUE']})
        MERGE (s:Subject {identifier:data_row['USUBJID']})
        MERGE (site:StudySite {name:site_row['SITEID']})
        MERGE (dc)<-[:FOR_DC_REL]-(d)
        MERGE (d)-[:FOR_SUBJECT_REL]->(s)
        MERGE (s)-[:ENROLLED_AT_SITE_REL]->(site)
        MERGE (site)<-[:MANAGES_SITE]-(researchOrg)
        MERGE (researchOrg)<-[:ORGANIZATIONS_REL]-(design)
        RETURN count(*)
    """
    results = db.query(query)
    print("results datapoints",results)

db = Neo4jConnection()

clear_created_nodes(db)
add_datapoints(db)

db.close()
