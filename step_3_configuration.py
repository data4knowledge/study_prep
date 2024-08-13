from pathlib import Path
from d4kms_service import Neo4jConnection
from model.configuration import Configuration



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


if __name__ == "__main__":
    print("Start")
    
    print("Done")
