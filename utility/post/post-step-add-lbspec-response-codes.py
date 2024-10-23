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

def execute_query(queryPath):
    # baseURL = "http://library.cdisc.org/api"
    baseURL = "https://api.library.cdisc.org/api"
    query = baseURL + queryPath
    print("query", query)
    response = requests.get(query, headers={'api-key': '51603f88b0784ac9aaba4e9c4f26e9ac', 'Accept': 'application/json'})
    try:
        print("parsing json")
        # json_data = json.loads(response.text)
        json_data = response.json()
        return {'json': json_data, 'response': response}
    except:
        print("Error in parsing json")


def get_bcs():
    bcs = []
    json_data = execute_query("/mdr/bc/packages/2022-10-26/biomedicalconcepts")
    if '_links' in json_data.keys():
        print("json_data['_links'].keys()",json_data['_links'].keys())
        if 'biomedicalConcepts' in json_data['_links'].keys():
            # print("BiomedicalConcepts",json_data['_links']['biomedicalConcepts'])
            for item in json_data['_links']['biomedicalConcepts']:
                # print(item)
                bcs.append(item['href'])    
    return bcs

def get_specialization_package():
    bcs = []
    # baseURL = "https://api.library.cdisc.org/api"
    # https://api.library.cdisc.org/api/cosmos/v2/mdr/specializations/sdtm/packages/{package}/datasetspecializations/{datasetspecialization}
    data = execute_query("/cosmos/v2/mdr/specializations/sdtm/packages/2023-07-06/datasetspecializations")
    print(len(data['json'])
    return json_data

def get_specialization():
    bcs = []
    # baseURL = "https://api.library.cdisc.org/api"
    # https://api.library.cdisc.org/api/cosmos/v2/mdr/specializations/sdtm/datasetspecializations/{dataset_specialization_id}
    # json_data = execute_query("/cosmos/v2/mdr/specializations/sdtm/datasetspecializations/C64433")
    data = execute_query("/cosmos/v2/mdr/specializations/sdtm/datasetspecializations/SYSBP")
    print(len(data['json']))
    return data

    if '_links' in json_data.keys():
        print("json_data['_links'].keys()",json_data['_links'].keys())
        if 'biomedicalConcepts' in json_data['_links'].keys():
            # print("BiomedicalConcepts",json_data['_links']['biomedicalConcepts'])
            for item in json_data['_links']['biomedicalConcepts']:
                # print(item)
                bcs.append(item['href'])    
    return bcs


def do():
  debug = []

  json_data = get_specialization()
  # json_data = get_specialization_package()
  debug .append(json_data)
  write_tmp('lbspec_debug.txt', debug)

  return
  db = Neo4jConnection()
  test_identifiers = []
  test_labels = {}
  with db.session() as session:
      query = """
        match (bc:BiomedicalConcept)-[:CODE_REL]->(ac)-[:STANDARD_CODE_REL]-(c:Code)
        return distinct bc.name as name, c.code as code
      """
      response = session.run(query)
      result = [x.data() for x in response]
      print("result[0].keys()",result[0].keys())
  db.close()
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


if __name__ == "banan":
  ct = CTService()
  # response = ct.index()
  # response = ct.codelist_index()
  response = ct.find_notation("SYSBP")
  print("respons[0]['parent'].keys()",response[0]['parent'].keys())
  # sdtm_cl = [cli for cli in response if 'SDTM' in cli['parent']['pref_label']]
  first_sdtm_term = next((cli for cli in response if 'SDTM' in cli['parent']['pref_label']),[])
  print(f"term found {first_sdtm_term['parent']['identifier']}.{first_sdtm_term['child']['identifier']}")

  response = ct.find_by_identifier(first_sdtm_term['child']['identifier'])
  first_sdtm_label = next((cli for cli in response if 'Test Name' in cli['parent']['pref_label']),[])
  print("first_sdtm_label",first_sdtm_label['child']['pref_label'])

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
