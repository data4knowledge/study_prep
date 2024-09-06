import json
from pathlib import Path
from d4kms_service import Neo4jConnection
from utility.debug import write_debug, write_tmp, write_tmp_json, write_define_json, write_define_xml, write_define_xml2
import xmlschema
from lxml import etree
from bs4 import BeautifulSoup as bs

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


ODM_XML = Path.cwd() / "data" / "odm" / "odm.xml"
print(".. ")
print("odm.xml file ", ODM_XML)
ODM_XLS = Path.cwd() / "data" / "odm" / "stylesheets" / "crf_1_3_2.xsl"
# DEFINE_HTML = Path.cwd() / "tmp" / "define.html"
# ODM_XML = "/Users/johannes/Library/CloudStorage/OneDrive-data4knowledge/shared_mac/standards/define-xml/DefineV217_0/examples/DefineXML-2-1-SDTM/defineV21-SDTM.xml"

def check_odm():
    from pprint import pprint
    schema_file = '/Users/johannes/Library/CloudStorage/OneDrive-data4knowledge/shared_mac/standards/define-xml/DefineV217_0/schema/cdisc-define-2.1/define2-1-0.xsd'
    schema = xmlschema.XMLSchema(schema_file)
    define_file = ODM_XML
    a = schema.to_dict(define_file)
    # pprint(schema.to_dict(define_file))

def xml_to_html():
  dom = etree.parse(ODM_XML)
  xslt =etree.parse(ODM_XLS)
  transform = etree.XSLT(xslt)
  newdom = transform(dom)
  raw_html = etree.tostring(newdom, pretty_print=True)
  # print(etree.tostring(newdom, pretty_print=True))

  # root = etree.tostring(raw_html) #convert the generated HTML to a string
  soup = bs(raw_html, features="lxml")                #make BeautifulSoup
  prettyHTML = soup.prettify()


  with open(DEFINE_HTML,'w') as f:
        # f.write(str(html))
        f.write(prettyHTML)

if __name__ == "__main__":
    # check_crm_links()
    check_odm()
    # xml_to_html()
