# import os
# import csv
from pathlib import Path
from d4kms_service import Neo4jConnection


print("\033[H\033[J") # Clears terminal window in vs code

def clear_created_nodes(db):
    query = "match (n:Datapoint) detach delete n return count(n)"
    results = db.query(query)
    print("results Datapoint",results)
    query = "match (n:DataPoint) detach delete n return count(n)"
    results = db.query(query)
    print("results DataPoint",results)
    query = "match (n:Subjec) detach delete n return count(n)"
    results = db.query(query)
    print("results Subjec!!!",results)
    query = "match (n:Subject) detach delete n return count(n)"
    results = db.query(query)
    print("results Subject",results)
    query = "match (n:StudySite) detach delete n return count(n)"
    results = db.query(query)
    print("results StudySite",results)
    # query = 'match (n:DataContract {delete:"me"}) detach delete n return count(n)'
    # results = db.query(query)
    # # print("results2",results)
    # query = 'match ()-[r:DC_TO_MAIN_TIMELINE]-() detach delete r return count(r)'
    # results = db.query(query)
    # # print("results4",results)

def add_datapoints(db):
    query = """
        LOAD CSV WITH HEADERS FROM 'file:///datapoints.csv'  AS data_row
        WITH data_row
        LOAD CSV WITH HEADERS FROM 'file:///enrolment.csv'  AS site_row 
        with data_row, site_row
        MATCH(dc:DataContract {uri:data_row['DC_URI']})
        MATCH(design:StudyDesign {name:'Study Design 1'})
        MERGE(d:DataPoint {value: data_row['DATAPOINT']})
        MERGE(s:Subject {identifier:data_row['USUBJID']})
        MERGE(site:StudySite {name:site_row['SITEID']})
        MERGE(dc)<-[:FOR_DC_REL]-(d)-[:FOR_SUBJECT_REL]->(s)-[:ENROLLED_AT_SITE_REL]->(site)<-[:MANAGES_SITE]-(researchOrg)<-[:ORGANIZATIONS_REL]-(design)
    """
    query = """
        LOAD CSV WITH HEADERS FROM 'file:///datapoints.csv'  AS data_row
        WITH data_row
        LOAD CSV WITH HEADERS FROM 'file:///enrolment.csv'  AS site_row 
        with data_row, site_row
        MATCH(dc:DataContract {uri:data_row['DC_URI']})
        MATCH(design:StudyDesign {name:'Study Design 1'})
        MERGE(d:DataPoint {value: data_row['DATAPOINT']})
        MERGE(s:Subject {identifier:data_row['USUBJID']})
        MERGE(site:StudySite {name:site_row['SITEID']})
        MERGE(dc)<-[:FOR_DC_REL]-(d)-[:FOR_SUBJECT_REL]->(s)-[:ENROLLED_AT_SITE_REL]->(site)<-[:MANAGES_SITE]-(researchOrg)<-[:ORGANIZATIONS_REL]-(design)
        return count(d)
    """
    query = """
        LOAD CSV WITH HEADERS FROM 'file:///datapoints.csv'  AS data_row
        WITH data_row
        LOAD CSV WITH HEADERS FROM 'file:///enrolment.csv'  AS site_row 
        with data_row, site_row
        MATCH (dc:DataContract {uri:data_row['DC_URI']})
        MATCH (design:StudyDesign {name:'Study Design 1'})
        MERGE (d:DataPoint {uri: data_row['DATAPOINT'], value: data_row['VALUE']})
        MERGE (s:Subject {identifier:data_row['USUBJID']})
        MERGE (site:StudySite {name:site_row['SITEID']})
        MERGE (dc)<-[:FOR_DC_REL]-(d)
        MERGE (d)-[:FOR_SUBJECT_REL]->(s)
        MERGE (s)-[:ENROLLED_AT_SITE_REL]->(site)
        MERGE (site)<-[:MANAGES_SITE]-(researchOrg)
        MERGE (researchOrg)<-[:ORGANIZATIONS_REL]-(design)
    """
        # return count(*) as SUCCESS
    # print("query:",query)
    results = db.query(query)
    print("results datapoints",results)

db = Neo4jConnection()

clear_created_nodes(db)
add_datapoints(db)

db.close()
