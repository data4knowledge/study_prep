import requests
import json
from pathlib import Path
from d4kms_generic import ServiceEnvironment
from d4kms_service import Neo4jConnection

print("\033[H\033[J") # Clears terminal window in vs code

def write_tmp(name, data):
    TMP_PATH = Path.cwd() / "tmp" / "saved_debug"
    OUTPUT_FILE = TMP_PATH / name
    print("\nWriting tmp file...",OUTPUT_FILE.name,OUTPUT_FILE, end="")
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

def execute_query(base_url, queryPath):
    # https://api.library.cdisc.org/api/cosmos/v2/mdr/bc/biomedicalconcepts/{biomedicalconcept}
    # baseURL = "http://library.cdisc.org/api"
    query = base_url + queryPath
    # print("query", query)
    response = requests.get(query, headers={'api-key': '51603f88b0784ac9aaba4e9c4f26e9ac', 'Accept': 'application/json'})
    try:
        print("parsing json")
        # json_data = json.loads(response.text)
        json_data = response.json()
        return json_data
        # return {'json': json_data, 'response': response}
    except:
        print("Error in parsing json")

def make_bc_spz_ref_name(identifier):
  return f"bc_spz_ref_{identifier}"

def get_specializations_for_bc_id(identifier):
    bcs = []
    # base:https://api.library.cdisc.org/api url:/cosmos/v2/mdr/specializations/datasetspecializations?biomedicalconcept=C64433
    base_url = "https://api.library.cdisc.org/api"
    url = f"/cosmos/v2/mdr/specializations/datasetspecializations?biomedicalconcept={identifier}"
    # debug.append(f"url: {url}")
    file_name = make_bc_spz_ref_name(identifier)
    full_name = save_path / f"{file_name}.json"
    # debug.append(f"full_name: {full_name}");print("full_name", full_name)
    if full_name.exists():
      # debug.append(f"file already exists {full_name}");print(f"file already exists {full_name}")
      with open(full_name) as f:
          data = json.load(f)
      return data
    data = execute_query(base_url, url)
    with open(full_name, 'w') as f:
      f.write(json.dumps(data, indent = 2))
    return data

def make_bc_name(identifier):
  return f"bc_{identifier}"

def get_bc_with_identifier(identifier):
    bcs = []
    # base:https://api.library.cdisc.org/api url:/mdr/bc/biomedicalconcepts/C64431
    base_url = "https://api.library.cdisc.org/api/cosmos/v2"
    url = f"/mdr/bc/biomedicalconcepts/{identifier}"
    # debug.append(f"url: {url}")
    file_name = make_bc_name(identifier)
    full_name = save_path / f"{file_name}.json"
    # debug.append(f"full_name: {full_name}");print("full_name", full_name)
    if full_name.exists():
      # debug.append(f"file already exists {full_name}");print(f"file already exists {full_name}")
      with open(full_name) as f:
          data = json.load(f)
      return data
    data = execute_query(base_url, url)
    with open(full_name, 'w') as f:
      f.write(json.dumps(data, indent = 2))
    return data

def make_bc_spz_name(identifier, title):
  return f"bc_{identifier}_spz_{title.replace(' ','_').replace('/','_')}"

def get_bc_spz_from_url(identifier, url, title):
    bcs = []
    base_url = "https://api.library.cdisc.org/api/cosmos/v2"
    # debug.append(f"url: {url}")
    file_name = make_bc_spz_name(identifier, title)
    # debug.append(f"file_name: {file_name}");print("file_name", file_name)
    full_name = save_path / f"{file_name}.json"
    # debug.append(f"full_name: {full_name}");print("full_name", full_name)
    if full_name.exists():
      # debug.append(f"file already exists {full_name}");print(f"file already exists {full_name}")
      with open(full_name) as f:
          data = json.load(f)
      return data
    data = execute_query(base_url, url)
    with open(full_name, 'w') as f:
      f.write(json.dumps(data, indent = 2))
    return data
    return

debug = []
save_path = Path('/Users/johannes/Library/CloudStorage/OneDrive-data4knowledge/shared_mac/standards/api/bc')

def main():
  # Download files with BC identifiers to get actual specialization file
  bcs = [
    'C64433',
    'C64431',
    'C64432',
    'C64467',
    'C64547',
    'C64853',
    'C64809',
    'C64849',
  ]
  bcs = [
    'C64433',
    'C64431',
    'C64432',
  ]
  ct = CTService()
  clis = []
  for bc in bcs:
    # Download bc file
    debug.append(f"\n--bc {bc}--")
    data = get_bc_with_identifier(bc)

    # Get specializations for bc
    data = get_specializations_for_bc_id(bc)
    debug.append("--sdtm specialization--")
    for link in data['_links']['datasetSpecializations']['sdtm']:
      # print("\n--sdtm--")
      # debug.append(f"len(links): {len(links)}")
      # debug.append(f"link['href']: {link['href']}")

      # Download specialization files
      spz = get_bc_spz_from_url(bc, link['href'], link['title'])
      spec = next((i for i in spz['variables'] if i['name'] == 'LBSPEC'),[])
      # debug.append(f"spec: {spec}")
      # debug.append(f"spec['assignedTerm']: {spec['assignedTerm']}")
      debug.append(f"spec['assignedTerm']['conceptId']: {spec['assignedTerm']['conceptId']}")
      # for v in spec['codelist'].items():
      #   debug.append(f"{v}")
      e = next((i for i in clis if i['code'] == spec['assignedTerm']['conceptId']), None)
      if e:
         pass
      else:
        response = ct.find_by_identifier(spec['assignedTerm']['conceptId'])
        for x in response:
          debug.append(f"x: {x}")
          debug.append(f"x['child']: {x['child']}")
          clis.append({'code': x['child']['identifier'],'notation': x['child']['notation'],'pref_label': x['child']['pref_label'],'decode': x['child']['name']})
      # first_sdtm_label = next((cli for cli in response if 'Test Name' in cli['parent']['pref_label']),[])
      # if first_sdtm_label:
      #   test_labels[item['identifier']] = first_sdtm_label['child']['pref_label']
      # else:
      #   print(f" {item} z not found")

  debug.append(f"\n--clis --")
  for x in clis:
     debug.append(x)

    # debug.append("\n--parentBiomedicalConcept--")
    # debug.append(f"data['_links']['parentBiomedicalConcept'].keys(): {data['_links']['parentBiomedicalConcept'].keys()}")
    # debug.append(f"data['_links']['parentBiomedicalConcept']['href']: {data['_links']['parentBiomedicalConcept']['href']}")
    # debug.append(f"data['_links']['parentBiomedicalConcept']['title']: {data['_links']['parentBiomedicalConcept']['title']}")



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
  main()
