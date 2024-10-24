import requests
import json
from pathlib import Path
from d4kms_generic import ServiceEnvironment
from d4kms_service import Neo4jConnection
from uuid import uuid4

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

def get_bcs_in_study_service(ids):
  db = Neo4jConnection()
  with db.session() as session:
    query = """
      match (bc:BiomedicalConcept)-[:CODE_REL]->(ac)-[:STANDARD_CODE_REL]-(c:Code)
      where c.code in %s
      match (bc)-[:PROPERTIES_REL]-(bcp:BiomedicalConceptProperty)
      where bcp.name = 'LBSPEC'
      return distinct bc.name as name, c.code as code, bcp.name as bcp_name
    """ % (ids)
    print("query", query)
    response = session.run(query)
    result = [x.data() for x in response]
  db.close()
  return result

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

def download_and_get_terms_for_lbspec(bcs):
  # bcs = [bc for bc in bcs if bc['name'] == 'Alkaline Phosphatase Concentration in Serum/Plasma']
  ct = CTService()
  clis = {}
  result = []
  for bc in bcs:
    item = {}
    item['name'] = bc['name']
    item['code'] = bc['code']
    item['bcp_name'] = bc['bcp_name']
    item['clis'] = []

    # Download bc file
    debug.append(f"\n--bc {bc['code']}--")
    data = get_bc_with_identifier(bc['code'])

    # Get specializations for bc
    data = get_specializations_for_bc_id(bc['code'])
    debug.append("--sdtm specialization--")
    # bc_clis[bc['code']] = []
    for link in data['_links']['datasetSpecializations']['sdtm']:
      # Download specialization files
      spz = get_bc_spz_from_url(bc['code'], link['href'], link['title'])
      specimen = next((i for i in spz['variables'] if i['name'] == 'LBSPEC'),[])
      debug.append(f"specimen: {specimen['assignedTerm']['conceptId']} {link['title']}")
      debug.append(f"finding: {specimen['assignedTerm']['conceptId']}")
      if specimen['assignedTerm']['conceptId'] in clis:
        debug.append(f"already have term: {specimen['assignedTerm']['conceptId']}")
        item['clis'].append(clis[specimen['assignedTerm']['conceptId']])
      else:
        debug.append(f"find term: {specimen['assignedTerm']['conceptId']}")
        response = ct.find_by_identifier(specimen['assignedTerm']['conceptId'])
        debug.append(f"- response: {len(response)}")
        first_response = response[0]
        debug.append(f"first_response['child']: {first_response['child']['identifier']} - {first_response['child']['notation']} - {first_response['child']['pref_label']}")
        cli = {'code': first_response['child']['identifier'],'notation': first_response['child']['notation'],'pref_label': first_response['child']['pref_label'],'decode': first_response['child']['name']}
        clis[specimen['assignedTerm']['conceptId']] = cli
        item['clis'].append(cli)
        # for x in response:
          # debug.append(f"x: {x}")
    item['clis'] = list({v['code']:v for v in item['clis']}.values())
    result.append(item)

  debug.append(f"\n--clis --")
  for x in clis:
     debug.append(x)

  debug.append(f"\n--bc with cli --")
  for x in result:
     debug.append(x)
    #  debug.append(f"{x['name']} {[i['notation'] for i in x['clis']]}")

  write_tmp('lbspec_debug.txt', debug)

  return

def delete_lbspec_codes():
  db = Neo4jConnection()
  with db.session() as session:
    query = "match (n) where n.lbspec = 'y' detach delete n return count(n) as n"
    debug.append(f"query: {query}")
    response = session.run(query)
    result = [x.data() for x in response]
    debug.append(f"deleted old nodes: {result}");print(f"deleted old nodes: {result}")
  db.close()

def do():
  db = Neo4jConnection()
  with db.session() as session:
    query = """
        match (bc:BiomedicalConcept {uuid:'79a00145-622f-4c2c-90da-0de68f253f69'})
        return *
    """
    debug.append(f"query: {query}")
    # response = session.run(query)
    # result = [x.data() for x in response]
    # debug.append(f"deleted old nodes: {result}");print(f"deleted old nodes: {result}")
  db.close()

def add_lbspec_codes():
  raw_bc_lbspec_codes = [
    {'name': 'Alanine Aminotransferase Concentration in Serum/Plasma', 'code': 'C64433', 'bcp_name': 'LBSPEC', 'clis': [{'code': 'C105706', 'notation': 'SERUM OR PLASMA', 'pref_label': 'Serum or Plasma', 'decode': 'Serum or Plasma'}]},
    {'name': 'Albumin Presence in Urine', 'code': 'C64431', 'bcp_name': 'LBSPEC', 'clis': [{'code': 'C105706', 'notation': 'SERUM OR PLASMA', 'pref_label': 'Serum or Plasma', 'decode': 'Serum or Plasma'}, {'code': 'C13283', 'notation': 'URINE', 'pref_label': 'Urine', 'decode': 'Urine'}]},
    {'name': 'Alkaline Phosphatase Concentration in Serum/Plasma', 'code': 'C64432', 'bcp_name': 'LBSPEC', 'clis': [{'code': 'C105706', 'notation': 'SERUM OR PLASMA', 'pref_label': 'Serum or Plasma', 'decode': 'Serum or Plasma'}]},
    {'name': 'Aspartate Aminotransferase in Serum/Plasma', 'code': 'C64467', 'bcp_name': 'LBSPEC', 'clis': [{'code': 'C105706', 'notation': 'SERUM OR PLASMA', 'pref_label': 'Serum or Plasma', 'decode': 'Serum or Plasma'}]},
    {'name': 'Creatinine Concentration in Urine', 'code': 'C64547', 'bcp_name': 'LBSPEC', 'clis': [{'code': 'C105706', 'notation': 'SERUM OR PLASMA', 'pref_label': 'Serum or Plasma', 'decode': 'Serum or Plasma'}, {'code': 'C13283', 'notation': 'URINE', 'pref_label': 'Urine', 'decode': 'Urine'}]},
    {'name': 'Potassium Concentration in Urine', 'code': 'C64853', 'bcp_name': 'LBSPEC', 'clis': [{'code': 'C12434', 'notation': 'BLOOD', 'pref_label': 'Blood', 'decode': 'Blood'}, {'code': 'C105706', 'notation': 'SERUM OR PLASMA', 'pref_label': 'Serum or Plasma', 'decode': 'Serum or Plasma'}, {'code': 'C13283', 'notation': 'URINE', 'pref_label': 'Urine', 'decode': 'Urine'}]},
    {'name': 'Sodium Concentration in Urine', 'code': 'C64809', 'bcp_name': 'LBSPEC', 'clis': [{'code': 'C12434', 'notation': 'BLOOD', 'pref_label': 'Blood', 'decode': 'Blood'}, {'code': 'C105706', 'notation': 'SERUM OR PLASMA', 'pref_label': 'Serum or Plasma', 'decode': 'Serum or Plasma'}, {'code': 'C13283', 'notation': 'URINE', 'pref_label': 'Urine', 'decode': 'Urine'}]},
    {'name': 'Hemoglobin A1C Concentration in Blood', 'code': 'C64849', 'bcp_name': 'LBSPEC', 'clis': [{'code': 'C12434', 'notation': 'BLOOD', 'pref_label': 'Blood', 'decode': 'Blood'}]},
  ]
  bc_lbspec_codes = [
    {'name': 'Alanine Aminotransferase Concentration in Serum/Plasma', 'code': 'C64433', 'bcp_name': 'LBSPEC', 'clis': [{'code': 'C105706', 'notation': 'SERUM OR PLASMA', 'pref_label': 'Serum or Plasma', 'decode': 'Serum or Plasma'}]},
    {'name': 'Albumin Presence in Urine', 'code': 'C64431', 'bcp_name': 'LBSPEC', 'clis': [{'code': 'C13283', 'notation': 'URINE', 'pref_label': 'Urine', 'decode': 'Urine'}]},
    {'name': 'Alkaline Phosphatase Concentration in Serum/Plasma', 'code': 'C64432', 'bcp_name': 'LBSPEC', 'clis': [{'code': 'C105706', 'notation': 'SERUM OR PLASMA', 'pref_label': 'Serum or Plasma', 'decode': 'Serum or Plasma'}]},
    {'name': 'Aspartate Aminotransferase in Serum/Plasma', 'code': 'C64467', 'bcp_name': 'LBSPEC', 'clis': [{'code': 'C105706', 'notation': 'SERUM OR PLASMA', 'pref_label': 'Serum or Plasma', 'decode': 'Serum or Plasma'}]},
    {'name': 'Creatinine Concentration in Urine', 'code': 'C64547', 'bcp_name': 'LBSPEC', 'clis': [{'code': 'C13283', 'notation': 'URINE', 'pref_label': 'Urine', 'decode': 'Urine'}]},
    {'name': 'Potassium Concentration in Urine', 'code': 'C64853', 'bcp_name': 'LBSPEC', 'clis': [{'code': 'C13283', 'notation': 'URINE', 'pref_label': 'Urine', 'decode': 'Urine'}]},
    {'name': 'Sodium Concentration in Urine', 'code': 'C64809', 'bcp_name': 'LBSPEC', 'clis': [{'code': 'C13283', 'notation': 'URINE', 'pref_label': 'Urine', 'decode': 'Urine'}]},
    {'name': 'Hemoglobin A1C Concentration in Blood', 'code': 'C64849', 'bcp_name': 'LBSPEC', 'clis': [{'code': 'C12434', 'notation': 'BLOOD', 'pref_label': 'Blood', 'decode': 'Blood'}]},
  ]

  db = Neo4jConnection()
  with db.session() as session:
    for i, item in enumerate(bc_lbspec_codes):
      debug.append(f"\n{item['name']}")
      for j, cli in enumerate(item['clis']):
        rc_uuid = str(uuid4())
        c_uuid = str(uuid4())
        c_params = [f"c.{k}='{v}'" for k,v in cli.items()]
        # debug.append(f"c_params: {c_params}")#;print("c_params", c_params)
        c_params_str = ", ".join(c_params)
        # debug.append(f"c_params_str: {c_params_str}")#;print("c_params_str", c_params_str)
        query = """
          match (bc:BiomedicalConcept)-[:PROPERTIES_REL]-(bcp:BiomedicalConceptProperty)
          where bc.name = '%s' and bcp.name = '%s'
          with bcp
          create (rc:ResponseCode {uuid:'%s'})
          SET rc.id = 'LBSPEC_%s_%s'
          SET rc.instanceType = 'ResponseCode'
          SET rc.isEnabled = True
          SET rc.lbspec = "y"
          create (c:Code {uuid:'%s'})
          SET c.lbspec = "y"
          SET %s
          create (bcp)-[r1:RESPONSE_CODES_REL]->(rc)-[r2:CODE_REL]->(c)
          set r1.fake_relationship = "yes"
          set r2.fake_relationship = "yes"
          return count(rc) as count_rc, count(c) as count_c
        """ % (item['name'], item['bcp_name'], rc_uuid, i, j , c_uuid, c_params_str)
        debug.append(f"query: {query}")
        response = session.run(query)
        result = [x.data() for x in response]
        debug.append(f"added item: {item['name']} - {cli['pref_label']}")
        # for x in result:
        #   debug.append(f"--: {x}")
  db.close()




if __name__ == "__main__":
  # Delete added lbspec terms
  delete_lbspec_codes()

  # # Get BCs lacking LBSPEC terminology
  # # Download files with BC identifiers to get actual specialization file
  # bc_ids = ['C64433','C64431','C64432','C64467','C64547','C64853','C64809','C64849']
  # # bc_ids = bc_ids[0:2]
  # bcs = get_bcs_in_study_service(bc_ids)
  # debug.append('-- starting with bc list --')
  # for x in bcs:
  #     debug.append(x)

  # download_and_get_terms_for_lbspec(bcs)
  # add_lbspec_codes()
  # write_tmp('lbspec_debug.txt', debug)
  # do()
