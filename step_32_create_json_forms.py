import json
import copy
import traceback
from pathlib import Path
from model.configuration import Configuration, ConfigurationNode
from d4kms_service import Neo4jConnection
from model.base_node import BaseNode
from utility.debug import write_debug, write_tmp, write_tmp_json, write_define_json, write_define_xml, write_define_xml2
from utility.define_query import get_activities_query, define_vlm_query, crm_link_query, _add_missing_links_to_crm_query, study_info_query, domains_query, domain_variables_query, variables_crm_link_query, define_codelist_query, define_test_codes_query, find_ct_query
from datetime import datetime
import xmlschema
import xml.etree.ElementTree as ET
from lxml import etree
from bs4 import BeautifulSoup as bs

# NOTE: Length

# NOTE: Origin. Needs study builder
# ['Assigned', 'Collected', 'Derived', 'Not Available', 'Other', 'Predecessor', 'Protocol']
# ['Investigator', 'Sponsor', 'Subject', 'Vendor']

# NOTE: DataType in DB: coding
# define-xml datatypes:
# ['integer', 'float', 'date', 'datetime', 'time', 'text', 'string', 'double', 'URI', 'boolean', 'hexBinary', 'base64Binary', 'hexFloat', 'base64Float', 'partialDate', 'partialTime', 'partialDatetime', 'durationDatetime', 'intervalDatetime', 'incompleteDatetime', 'incompleteDate', 'incompleteTime']

# NOTE: code, decode (TESTCD, TEST) seems to be in different places
# Adverse Event Prespecified:
#   - BC.name/label = Adverse Event Prespecified
#   - BC-[:CODE_REL]-(alias)-[:STANDARD_CODE_REL]-(code).decode = Solicited Adverse Event
# Systolic Blood Pressure: 
#   - BC.name/label = Systolic Blood Pressure
#   - BC-[:CODE_REL]-(alias)-[:STANDARD_CODE_REL]-(code).decode = SYSBP

DATATYPES = {
   'coding': 'string',
   'quantity': 'float',
   'Char': 'text',
   'Num': 'integer',
}


# ISSUE: Should be in DB. Could add to configuration
ORDER_OF_DOMAINS = [
  'TRIAL DESIGN',
  'SPECIAL PURPOSE',
  'INTERVENTIONS',
  'EVENTS',
  'FINDINGS',
  'FINDINGS ABOUT',
  'RELATIONSHIP',
  'STUDY REFERENCE',
]

# All possible classes
# ['ADAM OTHER', 'BASIC DATA STRUCTURE', 'DEVICE LEVEL ANALYSIS DATASET', 'EVENTS', 'FINDINGS', 'FINDINGS ABOUT', 'INTERVENTIONS', 'MEDICAL DEVICE BASIC DATA STRUCTURE', 'MEDICAL DEVICE OCCURRENCE DATA STRUCTURE', 'OCCURRENCE DATA STRUCTURE', 'REFERENCE DATA STRUCTURE', 'RELATIONSHIP', 'SPECIAL PURPOSE', 'STUDY REFERENCE', 'SUBJECT LEVEL ANALYSIS DATASET', 'TRIAL DESIGN']
# ISSUE: Should be in DB
# ISSUE: 'SPECIAL-PURPOSE' -> 'SPECIAL PURPOSE'
DOMAIN_CLASS = {
  'EVENTS'          :['AE', 'BE', 'CE', 'DS', 'DV', 'HO', 'MH'],
  'FINDINGS'        :['BS', 'CP', 'CV', 'DA', 'DD', 'EG', 'FT', 'GF', 'IE', 'IS', 'LB', 'MB', 'MI', 'MK', 'MS', 'NV', 'OE', 'PC', 'PE', 'PP', 'QS', 'RE', 'RP', 'RS', 'SC', 'SS', 'TR', 'TU', 'UR', 'VS'],
  'FINDINGS ABOUT'  :['FA', 'SR'],
  'INTERVENTIONS'   :['AG', 'CM', 'EC', 'EX', 'ML', 'PR', 'SU'],
  'RELATIONSHIP'    :['RELREC', 'RELSPEC', 'RELSUB', 'SUPPQUAL'],
  'SPECIAL PURPOSE' :['CO', 'DM', 'SE', 'SM', 'SV'],
  'STUDY REFERENCE' :['OI'],
  'TRIAL DESIGN'    :['TA', 'TD', 'TE', 'TI', 'TM', 'TS', 'TV'],
}

DOMAIN_KEY_SEQUENCE = {
  'AE' : {'STUDYID': '1', 'USUBJID': '2', 'AEDECOD': '3', 'AESTDTC': '4'},
  'DM' : {'STUDYID': '1', 'USUBJID': '2'},
  'DS' : {'STUDYID': '1', 'USUBJID': '2', 'DSDECOD': '3', 'DSSTDTC': '4'},
  'EX' : {'STUDYID': '1', 'USUBJID': '2', 'EXTRT': '3', 'EXSTDTC': '4'},
#  'LB' : {'STUDYID': '1', 'USUBJID': '2', 'LBTESTCD': '3', 'LBSPEC': '4', 'VISITNUM': '5', 'LBTPTREF': '6', 'LBTPTNUM': '7'},
  'LB' : {'STUDYID': '1', 'USUBJID': '2', 'LBTESTCD': '3', 'LBSPEC': '4', 'VISITNUM': '5', 'LBDTC': '6'},
  'VS' : {'STUDYID': '1', 'USUBJID': '2', 'VSTESTCD': '3', 'VSSPEC': '4', 'VISITNUM': '5', 'VSTPTREF': '6', 'VSTPTNUM': '7'}
}

debug = []


def check_crm_links():
    db = Neo4jConnection()
    with db.session() as session:
      # print("crm",query)
      query = crm_link_query()
      results = session.run(query)
      crm_links = [r.data() for r in results]
      for x in crm_links:
        debug.append([v for k,v in x.items()])
    db.close()

# NOTE: Fix proper links when loading
def _add_missing_links_to_crm():
  db = Neo4jConnection()
  with db.session() as session:
    # If topic result (e.g. Date of Birth)
    # if bcp['name'] != copy_bc_name:
    # bcp_name = "Date of Birth"

    var_link_crm = {
        'BRTHDTC':'https://crm.d4k.dk/dataset/observation/observation_result/result/quantity/value'
       ,'RFICDTC':'https://crm.d4k.dk/dataset/common/period/period_start/date_time/value'
       ,'DSDECOD':'https://crm.d4k.dk/dataset/observation/observation_result/result/coding/code'
       ,'DSSTDTC':'https://crm.d4k.dk/dataset/common/period/period_start/date_time/value'
       ,'DSDTC'  :'https://crm.d4k.dk/dataset/common/date_time/date_time/value'
       ,'DSTERM' :'https://crm.d4k.dk/dataset/observation/observation_result/result/quantity/value'
       ,'VSPOS'  :'https://crm.d4k.dk/dataset/observation/position/coding/code'
       ,'VSLOC'  :'https://crm.d4k.dk/dataset/common/location/coding/code'
       ,'DMDTC'  :'https://crm.d4k.dk/dataset/common/date_time/date_time/value'
       ,'EXDOSFRQ': 'https://crm.d4k.dk/dataset/therapeutic_intervention/frequency/coding/code'
       ,'EXROUTE': 'https://crm.d4k.dk/dataset/therapeutic_intervention/route/coding/code'
       ,'EXTRT': 'https://crm.d4k.dk/dataset/therapeutic_intervention/description/coding/code'
       ,'EXDOSFRM': 'https://crm.d4k.dk/dataset/therapeutic_intervention/form/coding/code'
       ,'EXDOSE': 'https://crm.d4k.dk/dataset/therapeutic_intervention/single_dose/quantity/value'
       ,'EXDOSU': 'https://crm.d4k.dk/dataset/therapeutic_intervention/single_dose/quantity/unit'
       ,'EXSTDTC': 'https://crm.d4k.dk/dataset/common/period/period_start/date_time/value'
       ,'EXENDTC': 'https://crm.d4k.dk/dataset/common/period/period_end/date_time/value'
       ,'AESTDTC': 'https://crm.d4k.dk/dataset/common/period/period_start/date_time/value'
       ,'AEENDTC': 'https://crm.d4k.dk/dataset/common/period/period_end/date_time/value'
       ,'AERLDEV'  : 'https://crm.d4k.dk/dataset/adverse_event/causality/device'
       ,'AERELNST' : 'https://crm.d4k.dk/dataset/adverse_event/causality/non_study_treatment'
       ,'AEREL'    : 'https://crm.d4k.dk/dataset/adverse_event/causality/related'
       ,'AEACNDEV' : 'https://crm.d4k.dk/dataset/adverse_event/response/concomitant_treatment'
       ,'AEACNOTH' : 'https://crm.d4k.dk/dataset/adverse_event/response/other'
       ,'AEACN'    : 'https://crm.d4k.dk/dataset/adverse_event/response/study_treatment'
       ,'AESER'    : 'https://crm.d4k.dk/dataset/adverse_event/serious'
       ,'AESEV'    : 'https://crm.d4k.dk/dataset/adverse_event/severity'
       ,'AETERM'   : 'https://crm.d4k.dk/dataset/adverse_event/term'
       ,'AETOXGR'  : 'https://crm.d4k.dk/dataset/adverse_event/toxicity/grade'
    }

    for var,uri in var_link_crm.items():
      query = _add_missing_links_to_crm_query(uri, var)
      results = db.query(query)
      # print("crm query results",results)
      if results:
        pass
        # application_logger.info(f"Created link to CRM from {var}")
      else:
        # application_logger.info(f"Info: Failed to create link to CRM for {var}")
        print(f"Warning: Failed to create link to CRM for {var}")
        # print("query", query)
  db.close()

def get_study_info():
    db = Neo4jConnection()
    with db.session() as session:
      query = study_info_query()
      # debug.append(query)
      results = session.run(query)
      data = [r.data() for r in results]
    db.close()
    return data[0]

def get_domains(uuid):
    db = Neo4jConnection()
    with db.session() as session:
      query = domains_query(uuid)
      # print("domains query", query)
      results = session.run(query)
      data = [r['d'] for r in results]
    db.close()
    return data

def get_variables(uuid):
    db = Neo4jConnection()
    with db.session() as session:
      query = domain_variables_query(uuid)
      # print("variables query", query)
      results = session.run(query)
      # all_variables = [r['v'] for r in results]
      all_variables = [r['v'] for r in results.data()]
      required_variables = [v for v in all_variables if v['core'] == 'Req']
      expected_variables = [v for v in all_variables if v['core'] == 'Exp']
      vars_in_use = required_variables + expected_variables
      # CRM linked vars
      query = variables_crm_link_query(uuid)
      results = session.run(query)
      vlm_variables = [r['v'] for r in results.data()]
      for v in vlm_variables:
          if next((w for w in vars_in_use if w['name'] == v['name']),None):
            pass
          else:
            vlm_v = next((w for w in all_variables if w['name'] == v['name']),None)
            if vlm_v:
              vars_in_use.append(vlm_v)

    db.close()
    return vars_in_use
    return expected_variables
    return all_variables


def get_define_vlm(domain_uuid):
    db = Neo4jConnection()
    with db.session() as session:
      query = define_vlm_query(domain_uuid)
      # debug.append("vlm query")
      # debug.append(query)
      results = session.run(query)
      data = [r for r in results.data()]
      # debug.append("vlm--->")
      # for d in data:
      #    debug.append(d)
      # debug.append("vlm<---")
    db.close()
    return data

def get_define_codelist(domain_uuid):
    db = Neo4jConnection()
    with db.session() as session:
      query = define_codelist_query(domain_uuid)
      # debug.append("codelist query"); debug.append(query)
      results = session.run(query)
      data = [r for r in results.data()]
      # debug.append("codelist--->")
      # for d in data:
      #    debug.append(d)
      # debug.append("codelist<---")
    db.close()
    return data

def get_concept_info(identifiers):
    db = Neo4jConnection()
    with db.session() as session:
      query = find_ct_query(identifiers)
      # debug.append("ct_find query")
      # debug.append(query)
      results = session.run(query)
      data = [r for r in results.data()]
      # debug.append("codelist--->")
      # for d in data:
      #    debug.append(d)
      # debug.append("codelist<---")
    db.close()
    return data

def get_define_test_codes(domain_uuid):
    db = Neo4jConnection()
    with db.session() as session:
      query = define_test_codes_query(domain_uuid)
      # debug.append("test_codes query"); debug.append(query)
      results = session.run(query)
      data = [r for r in results.data()]
      # debug.append("test_codes--->")
      # for d in data:
      #    debug.append(d)
      # debug.append("test_codes<---")
    db.close()
    return data

def get_activities(domain_uuid):
    db = Neo4jConnection()
    with db.session() as session:
      query = get_activities_query(domain_uuid)
      # debug.append("test_codes query"); debug.append(query)
      results = session.run(query)
      activities_list = [r for r in results.data()]
    db.close()
    order_numbers = list(set([x['order'] for x in activities_list]))
    activities = []
    for order in order_numbers:
       item = {}
       item['order'] = order
       acts = [activity for activity in activities_list if activity['order'] == order]
       item['id'] = acts[0]['id']
       item['name'] = acts[0]['activity_name']
       if acts[0]['bc_name'] == None:
         item['items'] = []
       else:
         item['items'] = acts if acts else []

       activities.append(item)
    return activities

def pretty_string(text):
   return text.replace(' ','_')

def get_unique_vars(original_vars):
  # Don't want to modify original list, so make a copy of it
  vars = copy.deepcopy(original_vars)
  unique_vars = []
  for v in vars:
      if 'bc' in v:
        v.pop('bc')
      if 'bc_uuid' in v:
        v.pop('bc_uuid')
      if 'decodes' in v:
        v.pop('decodes')
      unique_vars.append(v)
  unique_vars = list({v['uuid']:v for v in unique_vars}.values())
  return unique_vars

def get_unique_var_decode(vars):
  unique_var_decodes = []
  for v in vars:
      if 'bc' in v:
        v.pop('bc')
      if 'bc_uuid' in v:
        v.pop('bc_uuid')
      if 'decodes' in v:
        v.pop('decodes')
      unique_var_decodes.append(v)
      # debug.append(f"  adding {v}")
  unique_var_decodes = list({v['testcd']:v for v in unique_var_decodes}.values())
  debug.append(f"  added {[x['testcd'] for x in unique_var_decodes]}")
  return unique_var_decodes


JSON_FILE = Path.cwd() / "data" / "json" / "study_forms.json"
# ODM_XLS = Path.cwd() / "data" / "define" / "stylesheets" / "define2-1.xsl"
# ODM_HTML = Path.cwd() / "data" / "odm" / "odm.html"
# DEFINE_XML = Path('/Users/johannes/dev/python/github/study_service/uploads/define.xml')

def generate_json():
  try:
    root = {}
    study_info = get_study_info()
    debug.append(f"study_info {study_info}")
    activities = get_activities(study_info['uuid'])
    activities = activities[0:8]
    for activity in activities:
       debug.append(activity)

    with open(JSON_FILE, 'w') as f:
        f.write(json.dumps(activities, indent = 2))



# {'act': {'instanceType': 'Activity', 'name': 'CT scan', 'description': '', 'label': 'CT scan', 'id': 'Activity_18', 'uuid': '67671a28-d1b8-4b27-835c-97c11e67111a'}, 'bc': None, 'bcp': None, 'dc': None, 'sai': {'instanceType': 'ScheduledActivityInstance', 'name': 'SCREEN1', 'description': '-', 'label': 'Screen One', 'id': 'ScheduledActivityInstance_9', 'uuid': 'b2fd136f-0c70-4d4e-9ae3-039f099a2d06'}, 'sd': {'instanceType': 'StudyDesign', 'name': 'Study Design 1', 'description': 'The main design for the study', 'id': 'StudyDesign_1', 'label': '', 'uuid': '47343f4e-c9e0-44cf-b5de-09c3baf455d5', 'rationale': 'The discontinuation rate associated with this oral dosing regimen was 58.6% in previous studies, and alternative clinical strategies have been sought to improve tolerance for the compound. To that end, development of a Transdermal Therapeutic System (TTS) has been initiated.'}}
# {'act': {'instanceType': 'Activity', 'name': 'Patient randomised', 'description': '', 'label': 'Patient randomised', 'id': 'Activity_12', 'uuid': 'ae8b6213-8726-4f31-9294-ddee8d9b2850'}, 'bc': None, 'bcp': None, 'dc': None, 'sai': {'instanceType': 'ScheduledActivityInstance', 'name': 'DOSE', 'description': '-', 'label': 'Dose', 'id': 'ScheduledActivityInstance_11', 'uuid': '45bb7fd7-1023-4ef4-8b4a-b02ecd682775'}, 'sd': {'instanceType': 'StudyDesign', 'name': 'Study Design 1', 'description': 'The main design for the study', 'id': 'StudyDesign_1', 'label': '', 'uuid': '47343f4e-c9e0-44cf-b5de-09c3baf455d5', 'rationale': 'The discontinuation rate associated with this oral dosing regimen was 58.6% in previous studies, and alternative clinical strategies have been sought to improve tolerance for the compound. To that end, development of a Transdermal Therapeutic System (TTS) has been initiated.'}}


    # if 0 == 1:
    #     root = ET.Element('ODM')
    #     odm_properties(root)
    #     study = set_study_info(study_name=study_info['study_name'])
    #     # Study -------->
    #     study.append(set_globalvariables(study_name=study_info['study_name'], study_description=study_info['rationale'], protocol_name=study_info['protocol_name']))

    #     # MetadataVersion -------->
    #     metadata = metadata_version(oid=study_info['uuid'], name=study_info['study_name'],description="This is some kind of description")

    #     # Protocol -------->
    #     metadata.append(protocol(study_info, activities))

    #     # StudyEventDef -------->
    #     seds = studyeventdefs(study_info, activities)
    #     for sed in seds:
    #         metadata.append(sed)

    #     # FormDef -------->
    #     fds = formdefs(study_info, activities)
    #     for fd in fds:
    #         metadata.append(fd)

    #     # ItemGroupDef -------->
    #     igds = form_itemgroupdefs(activities)
    #     for igd in igds:
    #         metadata.append(igd)

    #     # ItemDef -------->
    #     igds = form_itemdefs(activities)
    #     for igd in igds:
    #         metadata.append(igd)
    
    
    #     # # MetadataVersion <--------
    #     # # Study <--------
    #     study.append(metadata)
    #     root.append(study)

    #     debug.append(root)

    write_tmp("json-debug.txt",debug)
    return root

  except Exception as e:
    write_tmp("json-debug.txt",debug)
    print("Error",e)
    print(traceback.format_exc())
    debug.append(f"Error: {e}")

def main():
    json = generate_json()
    if json:
      print("done with json, time to save json")
      save_json(json)
      print("done with json")

if __name__ == "__main__":
    # check_crm_links()
    # _add_missing_links_to_crm()
    main()
    # check_odm()
