from neo4j import GraphDatabase
from d4kms_service import Neo4jConnection, ServiceEnvironment

def db_is_down():
    sv = ServiceEnvironment()
    uri = sv.get('NEO4J_URI')
    auth = (sv.get("NEO4J_USERNAME"), sv.get("NEO4J_PASSWORD"))
    try:
        with GraphDatabase.driver(uri, auth=auth) as driver:
            driver.verify_connectivity()
        return False
    except:
        return True

print("Connecting to Neo4j...",end="")
if db_is_down():
    print("is not running")
    exit()
print("connected")
