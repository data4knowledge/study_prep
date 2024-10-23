import requests
from pathlib import Path
from d4kms_generic import ServiceEnvironment
from d4kms_service import Neo4jConnection

print("\033[H\033[J") # Clears terminal window in vs code

def write_tmp(name, data):
    TMP_PATH = Path.cwd() / "tmp" / "saved_debug"
    OUTPUT_FILE = TMP_PATH / name
    print("Writing file...",OUTPUT_FILE.name,OUTPUT_FILE, end="")
    with open(OUTPUT_FILE, 'w') as f:
        for it in data:
            f.write(str(it))
            f.write('\n')
    print(" ...done")

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
  debug = []
  db = Neo4jConnection()
  bcp_validation_rules = [
     {'bc_name': 'Informed Consent Obtained', 'bcp_name': 'DSSTDTC', 'rule': {'simple':'not_future_date'}},
     {'bc_name': 'Date of Birth', 'bcp_name': 'BRTHDTC', 'rule': {'min': 18, 'max':45}}
  ]
  with db.session() as session:
    for rule in bcp_validation_rules:
      params = [f"r.{k}='{v}'" for k,v in rule['rule'].items()]
      print("params", params)
      params_str = ", ".join(params)
      print("params_str", params_str)

      query = """
        match (bc:BiomedicalConcept)-[:PROPERTIES_REL]->(bcp:BiomedicalConceptProperty)
        where bc.name = '%s'
        and   bcp.name = '%s'
        match (bcp)<-[:DATA_ENTRY_CONFIG]-(dec:DataEntryConfig)
        with dec
        MERGE (r:Rule {identifier: '%s/%s'})
        SET %s
        MERGE (dec)<-[:HAS_RULE]-(r)
        return *
      """ % (rule['bc_name'], rule['bcp_name'], rule['bc_name'], rule['bcp_name'], params_str)
      print("query", query)
      response = session.run(query)
      result = [x.data() for x in response]
      print("result[0].keys()",result[0].keys())
  db.close()
  for x in result:
     debug.append(x)

  write_tmp('debug_add_validation.txt', debug)

  return
  for x in result:
      debug.append(x)
      test_identifiers.append(x)
  write_tmp('lbspec_debug.txt', debug)

  return
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
  do()
  # print(f"ct._url: {ct._url}")
  print("klart")
