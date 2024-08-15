from pathlib import Path
from d4kms_service import Neo4jConnection


def clear_created_nodes():
  return
  db = Neo4jConnection()
  with db.session() as session:
      query = "match (n:Datapoint|DataPoint) detach delete n return count(n)"
      results = session.run(query)
      print("Removing Datapoint/DataPoint",results)
      query = "match (n:Subject) detach delete n return count(n)"
      results = session.run(query)
      print("Removing Subject",results)

def set_ae_datapoints_unassigned():
  db = Neo4jConnection()
  with db.session() as session:
    query = """
      MATCH (bc:BiomedicalConcept)-[:PROPERTIES_REL]->(bcp:BiomedicalConceptProperty)
      MATCH (bcp)<-[:PROPERTIES_REL]->(dc:DataContract)
      MATCH (dc)<-[:FOR_DC_REL]->(dp:DataPoint)
      WHERE bc.name = "Adverse Event Prespecified"
      WITH dp
      SET dp.status = "unassigned"
      return count(dp) as count
    """
    results = session.run(query)
    print("Set status unassigned",next(results))
  return

def get_visits():
  db = Neo4jConnection()
  with db.session() as session:
    query = """
      MATCH (bc:BiomedicalConcept)-[:PROPERTIES_REL]->(bcp:BiomedicalConceptProperty)
      MATCH (bcp)<-[:PROPERTIES_REL]->(dc:DataContract)
      MATCH (dc)<-[:FOR_DC_REL]->(dp:DataPoint)
      WHERE bc.name = "Adverse Event Prespecified"
      WITH dp
      SET dp.status = "unassigned"
      return count(dp) as count
    """
    results = session.run(query)
    items = [result.data() for result in results]

def match_ae_datapoints():
  db = Neo4jConnection()
  with db.session() as session:
      query = """

      """
      results = session.run(query)
      items = [result.data() for result in results]

  # with db.session() as session:
  #     for item in items:
  #         query = f"""
  #             MATCH (dc:DataContract {{uri:'{item['data_contract']}'}})
  #             WITH COUNT(dc) > 0  as dc_exists
  #             RETURN dc_exists as exist
  #         """
  #         # print(query)
  #         results = session.run(query)
  #         if results[0].data()['exist']:
  #             pass
  #         else:
  #             print("\n---\ndata_contract MISSING :",item['data_contract'])

if __name__ == "__main__":
  # clear_created_nodes()
  set_ae_datapoints_unassigned()
  # match_ae_datapoints()
