import requests
from d4kms_generic import ServiceEnvironment
from d4kms_service import Neo4jConnection
from uuid import uuid4

class CTService():

  def __init__(self):
    self.__api_url = ServiceEnvironment().get("CT_SERVER_URL")

  def api_get(self, url):
    headers =  {"Content-Type":"application/json"}
    response = requests.get("%s%s" % (self.__api_url, url), headers=headers)
    #print(response)
    return response.json()

  def find_notation(self, term, page=1, size=10, filter=""):
    path = f"v1/terms"
    url = f"{path}?notation={term}&page={page}&size={size}&filter={filter}"
    # print(f"URL: {url}")
    return self.api_get(url)

  def find_by_identifier(self, identifier, page=1, size=10, filter=""):
    path = f"v1/terms"
    url = f"{path}?identifier={identifier}&page={page}&size={size}&filter={filter}"
    # print(f"URL: {url}")
    return self.api_get(url)

def do():
  db = Neo4jConnection()
  test_identifiers = []
  test_labels = {}
  with db.session() as session:
      query = """
        match (bc:BiomedicalConcept)-[:CODE_REL]-(ac:AliasCode)-[:STANDARD_CODE_REL]-(c:Code)
        return c.code as identifier, bc.name as name
      """
      response = session.run(query)
      result = [x.data() for x in response]
      print("result[0].keys()",result[0].keys())
      for x in result:
        test_identifiers.append(x)
  db.close()
  ct = CTService()
  # for x in test_identifiers:
  #   print("x",x)

  for item in test_identifiers:
    response = ct.find_by_identifier(item['identifier'])
    first_sdtm_label = next((cli for cli in response if 'Test Name' in cli['parent']['pref_label']),[])
    if first_sdtm_label:
      test_labels[item['identifier']] = first_sdtm_label['child']['pref_label']
    else:
      print(f" {item} z not found")

  for identifier, label in test_labels.items():
    print("identifier",identifier, label)
#   print("klar")
#   print(response)
#   print("respons[0]['parent'].keys()",response[0]['parent'].keys())

if __name__ == "__main__":
  db = Neo4jConnection()
  codes = [
    {'code':'C41260', 'decode':	'ASIAN'},
    {'code':'C16352', 'decode':	'BLACK OR AFRICAN AMERICAN'},
    {'code':'C41219', 'decode':	'NATIVE HAWAIIAN OR OTHER PACIFIC ISLANDER'},
    {'code':'C43234', 'decode':	'NOT REPORTED'},
    {'code':'C17649', 'decode':	'OTHER'},
    {'code':'C17998', 'decode':	'UNKNOWN'},
    {'code':'C41261', 'decode':	'WHITE'},
  ]
  # codes = codes[0:1]
  
  with db.session() as session:
      code = 'C41259'
      for c in codes:
        query = """
          MATCH (bcp:BiomedicalConceptProperty {name:'Race'})
          MERGE (r:ResponseCode {id:'rcid_%s', instanceType:'ResponseCode', isEnabled: True, uuid: '%s'})
          MERGE (c:Code {code:'%s', codeSystem: 'http://www.cdisc.org', codeSystemVersion: '2023-12-15', decode:	'%s', id: 'cid_%s', instanceType: 'Code'})
          MERGE (bcp)-[:RESPONSE_CODES_REL]->(r)-[:CODE_REL]->(c)
          return "done" as done
        """ % (c['code'], c['code'], c['code'], c['decode'], c['code'])
        print('query', query)
        response = session.run(query)
        result = [x.data() for x in response]
        print("is it done?",result)
        # for x in result:
        #   test_identifiers.append(x)
  db.close()



  exit()

  ct = CTService()
  # print(f"ct._url: {ct._url}")

  # response = ct.index()
  # response = ct.codelist_index()

  # response = ct.find_notation("SYSBP")
  # print("respons[0]['parent'].keys()",response[0]['parent'].keys())
  # # sdtm_cl = [cli for cli in response if 'SDTM' in cli['parent']['pref_label']]
  # first_sdtm_term = next((cli for cli in response if 'SDTM' in cli['parent']['pref_label']),[])
  # print(f"term found {first_sdtm_term['parent']['identifier']}.{first_sdtm_term['child']['identifier']}")

  # response = ct.find_by_identifier(first_sdtm_term['child']['identifier'])
  # C49487
  response = ct.find_by_identifier("C49487")
  print(response)
  # first_sdtm_label = next((cli for cli in response if 'Test Name' in cli['parent']['pref_label']),[])
  # print("first_sdtm_label",first_sdtm_label['child']['pref_label'])

  # for cl in sdtm_cl:
  #   print("parent:",[y for (x,y) in cl['parent'].items() if x in ['identifier','notation','pref_label']])
  #   print("  child:",[y for (x,y) in cl['child'].items() if x in ['identifier','notation','pref_label']])
  #   break
  # for cl in response:
  #   # print("parent:",[(x,y) for (x,y) in cl['parent'].items() if x in ['identifier']])
  #   # print(x['parent'].keys())
  #   # print(x['child'].keys())
  # # print(len(response['items']))
  print("klart")
