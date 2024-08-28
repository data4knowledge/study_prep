import json
from pathlib import Path
from d4kms_service import Neo4jConnection
from utility.debug import write_debug, write_tmp, write_tmp_json, write_define_json, write_define_xml, write_define_xml2
import xmlschema

debug = []

def check_crm_links():
    db = Neo4jConnection()
    with db.session() as session:
      query = """
        MATCH (d:Domain)-[:PROPERTIES_REL]->(bcp:BiomedicalConceptProperty)
        MATCH (bc:BiomedicalConcept)-[:PROPERTIES_REL]->(bcp:BiomedicalConceptProperty)
        MATCH (bcp)-[:IS_A_REL]->(crm:CRMNode)
        MATCH (v:Variable)-[:IS_A_REL]->(crm)
        MATCH (d:Domain)-[:VARIABLE_REL]->(v)
        RETURN distinct d.name as domain, bcp.name as bcp, crm.sdtm as crm, v.name as variable
        order by domain, variable
      """
      # print("crm",query)
      results = session.run(query)
      crm_links = [r.data() for r in results]
      for x in crm_links:
        debug.append([v for k,v in x.items()])


# DEFINE_XML = Path.cwd() / "tmp" / "define.xml"
DEFINE_XML = Path.cwd() / "tmp" / "define.xml"
print(".. ")
print("check file ", DEFINE_XML)
# DEFINE_XML = "/Users/johannes/Library/CloudStorage/OneDrive-data4knowledge/shared_mac/standards/define-xml/DefineV217_0/examples/DefineXML-2-1-SDTM/defineV21-SDTM.xml"

def check_define():
    from pprint import pprint
    schema_file = '/Users/johannes/Library/CloudStorage/OneDrive-data4knowledge/shared_mac/standards/define-xml/DefineV217_0/schema/cdisc-define-2.1/define2-1-0.xsd'
    schema = xmlschema.XMLSchema(schema_file)
    define_file = DEFINE_XML
    a = schema.to_dict(define_file)
    # pprint(schema.to_dict(define_file))

if __name__ == "__main__":
    # check_crm_links()
    check_define()
