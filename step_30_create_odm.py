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

xml_header = """<?xml version="1.0" encoding="UTF-8"?>\n<?xml-stylesheet type="text/xsl" href="stylesheets/crf_1_3_2.xsl"?>\n"""


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

def odm_properties(root):
  # now = datetime.now().replace(tzinfo=datetime.timezone.utc).isoformat()
  now = datetime.now().isoformat()
  # Using a static time to be able to see differencec in generated output
  now = "2024-09-06T09:53:42.385602"
  root.set('xmlns',"http://www.cdisc.org/ns/odm/v1.3")
  # root.set('xmlns:xlink',"http://www.w3.org/1999/xlink")
  root.set('xmlns:def',"http://www.cdisc.org/ns/def/v2.1")
  root.set('ODMVersion',"1.3.2")
  root.set('FileOID',"odm.xml")
  root.set('FileType',"Snapshot")
  root.set('CreationDateTime',now)
  root.set('Originator',"Study Service")
  root.set('SourceSystem',"Study service")
  root.set('SourceSystemVersion',"Alpha1")
  root.set('def:Context',"Other")

  root.set('xmlns:sdm',"http://www.cdisc.org/ns/studydesign/v1.0")
  root.set('xmlns:v4',"http://www.viedoc.net/ns/v4")
  root.set('SourceSystemVersion',"alpha1")
  root.set('SourceSystem',"Study Service")
  root.set('Originator',"data4knowledge")
  root.set('AsOfDateTime',"2024-06-13T10:34:00.768Z")
  # root.set('v4:ModifiedSystemVersion',"4.78")
  root.set('Granularity',"Metadata")




def set_study_info(oid= 'tbd', study_name = 'tbd', description = 'tbd', protocol_name = 'tbd'):
  study = ET.Element('Study')
  study.set('OID',study_name)
  # study.set('StudyDescription', description)
  # study.set('ProtocolName', protocol_name)
  return study

# ISSUE: Hardcoded
def set_globalvariables(study_name = None, study_description = "Not set", protocol_name = "Not set"):
  global_variables = ET.Element('GlobalVariables')
  element = ET.Element('StudyName')
  element.text = study_name
  global_variables.append(element)
  element = ET.Element('StudyDescription')
  element.text = study_description
  global_variables.append(element)
  element = ET.Element('ProtocolName')
  element.text = protocol_name
  global_variables.append(element)
  return global_variables

def comment_def(oid, text, lang = 'en', leaf_id = None, page_refs = None, ref_type = None):
  c = ET.Element('def:CommentDef')
  c.set('OID', oid)
  d = ET.SubElement(c,'Description')
  d.append(translated_text(text, lang))
  # Add page reference
  return c
   
# ISSUE: Hardcoded
def comment_defs():
  comment_defs = []
  comment_def_oid = "COM.STD.1"
  comment = comment_def(comment_def_oid, "Yada yada yada")
  comment_defs.append(comment)
  return comment_defs

# ISSUE: Hardcoded
def standards():
  standards = ET.Element('def:Standards')

  standard1 = ET.Element('def:Standard')
  standard1.set("OID", "STD.1")
  standard1.set("Name", "SDTMIG")
  standard1.set("Type", "IG")
  standard1.set("Version", "3.4")
  standard1.set("Status", "Final")
  standard1.set("def:CommentOID", "COM.STD.1")
  standards.append(standard1)

  standard1 = ET.Element('def:Standard')
  standard1.set("OID", "STD.2")
  standard1.set("Name", "CDISC/NCI")
  standard1.set("Type", "CT")
  standard1.set("Version", "2023-12-09")
  standard1.set("Status", "Final")
  standards.append(standard1)

  return standards

def studyeventref(activity):
  ref = ET.Element('StudyEventRef')
  ref.set('StudyEventOID',activity['id'])
  ref.set('OrderNumber',str(activity['order']))
  ref.set('Mandatory','No')
  return ref

def activitydef_oid(activity):
  return f"ad_{activity['id']}"

def activitydef(activity, first = False):
  act_def = ET.Element('sdm:ActivityDef')
  act_def.set('OID',activitydef_oid(activity))
  # NOTE: Assuming that it can only be one = Informed Consent
  if activity['order'] == 1:
    act_def.set('v4:StartMethodOID','MD_FIRST_ACTIVITY')
  act_def.set('name',activity['id'])
  act_def.append(description('en',activity['id']))
  # for item in activity['items']:
  #   act_def.append(formref(item))
  act_def.append(formref(activity))
  return act_def

def activityref(item):
  ref = ET.Element('sdm:ActivityRef')
  ref.set('ActivityOID',activitydef_oid(item))
  return ref

def workflow(start_activity, last_activity, early_activity = None):
  workflow = ET.Element('sdm:Workflow')

  start = ET.Element('sdm:StudyStart')
  start.append(description('en','informed consent'))
  if start_activity:
    start.append(activityref(start_activity))
  workflow.append(start)

  end = ET.Element('sdm:StudyFinish')
  if last_activity:
     end.append(activityref(last_activity))
  end.append(description('en','last visit'))
  workflow.append(end)

  path_finish = ET.Element('sdm:PathCanFinish')
  path_finish.append(description('en','PathCanFinish'))
  if early_activity:
     path_finish.append(activityref(early_activity))
  workflow.append(path_finish)

  return workflow

def structure(activities):
  structure = ET.Element('sdm:Structure')
  # NOTE: Assuming that it can only be one = Informed Consent
  # structure.append(activitydef(activities[0], first=True))
  for activity in activities:
    structure.append(activitydef(activity))
  structure.append(workflow(activities[0],activities[-1]))
  timing = ET.Element('sdm:Timing')
  structure.append(timing)

  return structure

def protocol(study_info, activities):
  protocol = ET.Element('Protocol')
  protocol.append(description('en',study_info['description']))
  # for i, activity in enumerate(activities):
  for activity in activities:
     protocol.append(studyeventref(activity))
  summary = ET.Element('sdm:Summary')
  summary.append(description('en','Some other description'))
  protocol.append(summary)
  protocol.append(structure(activities))
  return protocol

def studyeventdefs(study_info, activities):
  seds = []
  for act in activities:
    d = ET.Element('StudyEventDef')
    d.append(description('en',act['name']))
    # for item in act['items']:
    #   d.append(formref(item))
    d.append(formref(act))
    for item in act['items']:
      d.append(activityref(item))
    seds.append(d)
  return seds

# def itemgroupdef_oid(activity):
  # return f"igd_{activity['name']}"
def itemgroupdef_oid(item):
  return f"igd_{item['bc_name']}"

def item_group_ref(item):
  ref = ET.Element('ItemGroupRef')
  ref.set('ItemGroupOID', itemgroupdef_oid(item))
  return ref

def formdef_oid(item):
  print(item.keys())
  return f"fd_{pretty_string(item['name'])}"

def formref(item):
  ref = ET.Element('FormRef')
  ref.set('FormOID', formdef_oid(item))
  ref.set('Mandatory', 'No')
  return ref

def formdefs(study_info, activities):
  fds = []
  has_items = [x for x in activities if x['items']]
  for activity in has_items:
    d = ET.Element('FormDef')
    d.set('OID', formdef_oid(activity))
    for item in activity['items']:
      d.append(item_group_ref(item))
      fds.append(d)
  return fds

def odm_item_def_oid(p):
    # return f"IT.{item['domain']}.{pretty_string(item['name'])}"
    return f"IT.{pretty_string(p['bcp'])}.{p['dc']}"

def item_ref(item):
  ir = ET.Element('ItemRef')
  ir.set('OID', odm_item_def_oid(item))
  ir.set('Mandatory', 'No')
  return ir

def form_itemgroupdefs(activities):
  igds = []
  has_items = [x for x in activities if x['items']]
  for activity in has_items:
    for item in activity['items']:
      igd = ET.Element('ItemGroupDef')
      # igd.set('OID', itemgroupdef_oid(activity))
      igd.set('OID', itemgroupdef_oid(item))
      for p in item['bcps']:
        igd.append(item_ref(p))
      igds.append(igd)
  return igds

def form_itemdefs(activities):
  idfs = []
  has_items = [x for x in activities if x['items']]
  for activity in has_items:
    for item in activity['items']:
      for p in item['bcps']:
        idf = ET.Element('ItemDef')
        idf.set('OID', odm_item_def_oid(p))
        question = ET.Element('Question')
        question.append(translated_text('en', p['bcp']))
        idf.append(question)
        # idf.append(item_ref(p))
        idfs.append(idf)
  return idfs



def metadata_version(oid = 'Not set', name = 'Not set', description = 'Not set'):
  metadata = ET.Element('MetaDataVersion')
  metadata.set("OID", oid)
  metadata.set("Name", name)
  metadata.set("Description", description)
  metadata.set("def:DefineVersion", "2.1.7")
  return metadata

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

def translated_text(text, language = 'en'):
    translated_text = ET.Element('TranslatedText')
    translated_text.set('xml:lang',language)
    translated_text.text = text
    return translated_text

def description(language, text_str):
    description = ET.Element('Description')
    description.append(translated_text(text_str, language))
    return description

def origin(type, source):
    origin = ET.Element('def:Origin')
    origin.set('Type', type)
    origin.set('Source', source)
    return origin

def leaf(id, href, text_str):
    leaf = ET.Element('def:leaf')
    leaf.set("ID", id)
    leaf.set("xlink:href", href)
    title = ET.SubElement(leaf, 'def:title')
    title.text = text_str
    return leaf

def set_variable_refs(variables):
    variable_refs = []
    for v in variables:
      ref = ET.Element('ItemRef')
      # debug.append(f"  variable_refs item_oid {item_def_oid(v)}")
      ref.set('ItemOID', item_def_oid(v))
      mandatory = 'No' if v['core'] == 'Perm' else 'Yes'
      ref.set('Mandatory', mandatory)
      order = int(v['ordinal'])
      ref.set('OrderNumber', str(order))
      if v['name'] in DOMAIN_KEY_SEQUENCE[v['domain']]:
        ref.set('KeySequence', DOMAIN_KEY_SEQUENCE[v['domain']][v['name']])
      variable_refs.append(ref)
    return variable_refs

def item_group_defs(domains):
    debug.append(f"--item_group_defs")
    igds = []
    for d in domains:
        igd = ET.Element('ItemGroupDef')
        igd.set('OID', d['uuid'])
        igd.set('Domain', d['name'])
        igd.set('Name', d['name'])
        igd.set('Repeating', 'No')
        igd.set('IsReferenceData', 'No')
        igd.set('SASDatasetName', d['name'])
        igd.set('def:Structure', d['structure'])
        igd.set('Purpose', 'Tabulation')
        igd.set('def:StandardOID', 'STD.1')
        igd.set('def:ArchiveLocationID', f"LI.{d['name']}")
        igd.append(description('en',d['label']))
        # ISSUE/Question: Why does the order matter? Had to move refs after description
        refs = set_variable_refs(d['variables'])
        for ref in refs:
          igd.append(ref)
        ET.SubElement(igd,'def:Class', {'Name': d['goc']})
        # ET.SubElement(igd,'def:Class').text = goc
        # goc_e.text = goc
        igd.append(leaf(f"LI.{d['name']}", d['name'].lower()+".xpt", d['name'].lower()+".xpt"))
        igds.append(igd)
    return igds

def item_def_oid(item):
    # return f"IT.{pretty_string(item['name'])}.{item['uuid']}"
    return f"IT.{item['domain']}.{pretty_string(item['name'])}"

def item_def_vlm_oid(item):
    # return f"ITC.{pretty_string(item['name'])}.{item['testcd']}.{item['uuid']}"
    return f"IDVO.{item['domain']}.{pretty_string(item['name'])}.{item['testcd']}"

def item_defs_variable(domains):
    debug.append(f"--item_defs_variable")
    idfs = []
    for d in domains:
        for item in d['variables']:
          # debug.append(f"2 item {item}")
          idf = ET.Element('ItemDef')
          idf.set('OID', item_def_oid(item))
          idf.set('Name', item['name'])
          datatype = DATATYPES[item['datatype']] if 'datatype' in item else ""
          if datatype == "":
            # NOTE: Using SDTM datatype. Not always correct e.g. VISITNUM
            datatype = DATATYPES[item['data_type']]
          if datatype == "":
             print("-- item_defs_variable CHECK", item['name'])
          idf.set('DataType', datatype)
          idf.set('Length', '8')
          idf.set('SASFieldName', item['name'])
          idf.append(description('en',item['label']))
          if d['goc'] in ['FINDINGS','FINDINGS ABOUT']:
            # If variable has vlm
            if next((x for x in d['vlm'] if x['uuid'] == item['uuid']), None):
              print("-- referencing valuelist ", d['name'], item['name'])
              debug.append(f"-- adding ValueListRef {d['name']} {item['name']}")
              vl_ref = ET.Element('def:ValueListRef')
              vl_ref.set('ValueListOID', value_list_oid(item))
              idf.append(vl_ref)
            # If variable only has codelist
            elif next((x for x in d['codelist'] if x['uuid'] == item['uuid']), None):
              debug.append(f"-- adding CodelistRef (abnormal) {d['name']} {item['name']}")
              # print("found codelist", d['name'], item['name'])
              cl_ref = ET.Element('CodeListRef')
              cl_ref.set('CodeListOID', codelist_oid(item))
              idf.append(cl_ref)
          else:
            if next((x for x in d['codelist'] if x['uuid'] == item['uuid']), None):
              debug.append(f"-- adding CodelistRef (normal) {d['name']} {item['name']}")
              # print("found codelist", d['name'], item['name'])
              cl_ref = ET.Element('CodeListRef')
              cl_ref.set('CodeListOID', codelist_oid(item))
              idf.append(cl_ref)

          idf.append(origin('Collected','Sponsor'))
              # print("Not referencing ", d['name'], item['name'])
            # <def:ValueListRef ValueListOID="VL.LB.LBORRES"/>

          idfs.append(idf)
    return idfs

def var_test_key(item):
   return f"{item['name']}.{item['testcd']}"

def vlm_item_defs(domains):
    debug.append(f"--vlm_item_defs")
    idfs = {}
    for d in domains:
      # debug.append(f"vlm_item_defs domain {d['name']}")
      if d['goc'] in ['FINDINGS','FINDINGS ABOUT']:
        for item in d['vlm']:
          key = var_test_key(item)
          if key in idfs:
            debug.append(f"  Ignoring key: {key}")
          else:
            debug.append(f"  Now adding item_def_test: {item}")
            idf = ET.Element('ItemDef')
            idf.set('OID', item_def_vlm_oid(item))
            idf.set('Name', f"{item['name']} {item['testcd']}")
            datatype = DATATYPES[item['datatype']] if 'datatype' in item else ""
            if datatype == "":
              # NOTE: Using SDTM datatype. Not always correct e.g. VISITNUM
              datatype = DATATYPES[item['data_type']]
            if datatype == "":
              print("-- vlm_item_defs CHECK", item['name'])
            idf.set('DataType', datatype)
            idf.set('Length', '8')
            idf.set('SASFieldName', item['name'])
            idf.append(description('en',item['label']))
            cl_ref = ET.Element('CodeListRef')
            cl_ref.set('CodeListOID', vlm_codelist_oid(item))
            idf.append(cl_ref)
            idf.append(origin('Collected','Sponsor'))
            idfs[key] = idf



      else:
        debug.append(f"  - Ignoring domain")
    debug.append(f"returning {list(idfs.values())}")
    return list(idfs.values())

def codelist_oid(item):
    return f"CL.{pretty_string(item['name'])}"

def test_codelist_oid(item):
    return f"CL.CL.{item['domain']}.{item['domain']}TESTCD"

def vlm_codelist_oid(item):
    return f"CL.{item['domain']}.{pretty_string(item['name'])}.{item['testcd']}"

def alias(context, code):
    a = ET.Element('Alias')
    a.set('Context', context)
    a.set('Name', code)
    return a

def enumerated_item(code, context, value):
    e = ET.Element('EnumeratedItem')
    e.set('CodedValue', value)
    e.append(alias(context, code))
    return e

def codelist_item(code, short, long, context):
    e = ET.Element('CodeListItem')
    e.set('CodedValue', short)
    d = ET.SubElement(e,'Decode')
    d.append(translated_text(long))
    if code:
      e.append(alias(context, code))
    return e

def codelist_name(item):
   if 'testcd' in item:
     return f"CL {item['name']} {item['testcd']} ({item['bc']})"
   else:
     return f"CL {item['name']}"

# Create codelists for domain variable
def codelist_defs(domains):
    debug.append(f"--codelist_defs")
    codelists = []
    for d in domains:
        for item in d['codelist']:
          cl = ET.Element('CodeList')
          cl.set('OID', codelist_oid(item))
          cl.set('Name', codelist_name(item))
          cl.set('def:StandardOID', "STD.2")
          datatype = DATATYPES[item['datatype']] if 'datatype' in item else ""
          if datatype == "":
            # NOTE: Using SDTM datatype. Not always correct e.g. VISITNUM
            datatype = DATATYPES[item['data_type']]
            # datatype = "string"
          datatype = "text"
          if datatype == "":
            print("-- codelist_defs CHECK", item['name'])
          cl.set('DataType', datatype)
          codes = [x['code'] for x in item['decodes']]
          clis = get_concept_info(codes)
          for cli in clis:
            # NOTE: Need to care for enumerated item?
            # cl.append(enumerated_item(x['code'], "nci:ExtCodeID",x['decode']))
            cl.append(codelist_item(cli['code'], cli['notation'], cli['pref_label'], "nci:ExtCodeID"))
          codelists.append(cl)
    return codelists

def vlm_codelist_name(item):
  return f"CL {item['domain']} {item['name']} {item['testcd']}"

# Create codelist for VLM
def vlm_codelists_defs(domains):
    debug.append(f"--vlm_codelists_defs")
    vlm_codelists = {}
    for d in domains:
      if d['goc'] in ['FINDINGS','FINDINGS ABOUT']:
        for item in d['vlm']:
          key = var_test_key(item)
          if key in vlm_codelists:
            debug.append(f"  Ignoring key: {key}")
          else:
            debug.append(f"  Add vml_codelist: {key}")
            cl = ET.Element('CodeList')
            cl.set('OID', vlm_codelist_oid(item))
            cl.set('Name', vlm_codelist_name(item))
            cl.set('def:StandardOID', "STD.2")
            datatype = DATATYPES[item['datatype']] if 'datatype' in item else ""
            if datatype == "":
              # NOTE: Using SDTM datatype. Not always correct e.g. VISITNUM
              datatype = DATATYPES[item['data_type']]
            cl.set('DataType', datatype)
            cl.set('Length', '8')
            # cl.set('SASFieldName', item['name'])
            # cl.append(description('en',item['label']))
            # cl.append(origin('Collected','Sponsor'))
            codes = [x['code'] for x in item['decodes']]
            # for code in codes:
            #   debug.append(f"    code: {code}")
               
            clis = get_concept_info(codes)
            for cli in clis:
              # NOTE: Need to care for enumerated item?
              # cl.append(enumerated_item(x['code'], "nci:ExtCodeID",x['decode']))
              cl.append(codelist_item(cli['code'], cli['notation'], cli['pref_label'], "nci:ExtCodeID"))
            vlm_codelists[key] = cl

    return list(vlm_codelists.values())


def test_codelist_name(item):
   return f"CL {item['domain']} ({item['domain']+'TESTCD'})"

# Create codelist for TESTCD/TEST
def test_codes_defs(domains):
    debug.append(f"--test_codes_defs")
    test_codes = []
    for d in domains:
        # debug.append(f"test_codes_defs {d['name']}")
        if 'test_codes' in d:
          for item in d['test_codes']:
            debug.append(f"test_codes_defs {item}")
            cl = ET.Element('CodeList')
            cl.set('OID', test_codelist_oid(item))
            cl.set('Name', test_codelist_name(item))
            cl.set('def:StandardOID', "STD.1")
            cl.set('DataType', "text")
            # debug.append(f"1 codelist {item}")
            for test in item['test_codes']:
              # debug.append(f"testcodes {test}")
              # cl.append(enumerated_item(x['code'], "nci:ExtCodeID",x['decode']))
              cl.append(codelist_item(test['code'], test['testcd'], test['test'], "nci:ExtCodeID"))
            test_codes.append(cl)
        # debug.append(f"len(test_codes) {len(test_codes)}")
    return test_codes

def value_list_oid(item):
    # return f"VL.{item['name']}.{item['uuid']}"
    # return f"VL.{item['domain']}.{item['name']}.{item['uuid']}"
    return f"VL.{item['domain']}.{item['name']}"

def value_list_defs(domains):
    debug.append(f"--value_list_defs")
    vlds = []
    for d in domains:
        # debug.append(f"\nvalues_list_defs domain: {d['name']}")
        if d['goc'] in ['FINDINGS','FINDINGS ABOUT']:
          for v in d['variables']:
            vlms  = [x for x in d['vlm'] if x['uuid'] == v['uuid']]
            if vlms:
              # NOTE: Make one for all items for the variable
              # NOTE: Make one per test code (VLM)
              # debug.append(f"\nVariable: {v['name']}")
              # debug.append(f"len(vlm): {len(vlms)}")
              vld = ET.Element('def:ValueListDef')
              vld.set('OID', value_list_oid(v))
              item_refs = {}
              i = 1
              for vlm in vlms:
                key = var_test_key(vlm)
                if key in item_refs:
                  debug.append(f"  ignoring : {key}")
                else:
                  # debug.append(f"vlm: {vlm}")
                  item_ref = ET.Element('ItemRef')
                  # item_ref.set('ItemOID', f"{i}.{vlm['uuid']}")
                  # item_ref.set('ItemOID', item_def_oid(vlm))
                  # debug.append(f"  vld item_oid {item_def_oid(vlm)}")
                  # debug.append(f"  vld item_test_oid {item_def_vlm_oid(vlm)}")
                  item_ref.set('ItemOID', item_def_vlm_oid(vlm))
                  # item_ref.set('ItemOID', item_def_vlm_oid(vlm))
                  item_ref.set('OrderNumber', str(i))
                  item_ref.set('Mandatory', 'No')
                  wcd = ET.Element("def:WhereClauseRef")
                  wcd.set('WhereClauseOID', where_clause_oid(vlm)) 
                  debug.append(f"vld where oid {where_clause_oid(vlm)}")
                  item_ref.append(wcd)
                  i += 1
                  # item_refs.append(item_ref)
                  item_refs[key] = item_ref
                  # vld.append(item_ref)
                # debug.append(ET.dump(vld))
              for ref in item_refs.values():
                vld.append(ref)
              debug.append(f"vld {ET.tostring(vld)}")
              vlds.append(vld)
    return vlds

# def where_clause_oid(var_uuid, domain, variable, test):
def where_clause_oid(item):
    # wcd.set('WhereClauseOID', where_clause_oid(v['uuid'],d['name'], vlm['name'], vlm['testcd'])) 
    # wcd.set('OID',            where_clause_oid(v['uuid'],d['name'], v['name'], v['testcd']))
    return f"WC.{item['domain']}.{item['name']}.{item['testcd']}" #.{var_uuid}"

def range_check(decodes,comparator, soft_hard, item_oid):
    range_check = ET.Element('RangeCheck')
    range_check.set('Comparator', comparator)
    range_check.set('SoftHard', soft_hard)
    range_check.set('def:ItemOID', item_oid)
    if isinstance(decodes, list):
      for decode in decodes:
        check_value = ET.Element('CheckValue')
        check_value.text = decode['decode']
        range_check.append(check_value)
    else:
        check_value = ET.Element('CheckValue')
        check_value.text = decodes
        range_check.append(check_value)
    return range_check

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


def where_clause_defs(domains):
    debug.append(f"--where_clause_defs")
    wcds = {}
    for d in domains:
        if d['goc'] in ['FINDINGS','FINDINGS ABOUT']:
          debug.append(f"\n where_clause_defs domain: {d['name']}")
          testcd_var = next((v for v in d['variables'] if v['name'] == d['name']+"TESTCD"),"Not found")
          testcd_oid = item_def_oid(testcd_var)
          for v in d['vlm']:
            key = var_test_key(v)
            if key in wcds:
              debug.append(f"  ignoring : {key}")
            else:
              debug.append(f"  doing : {key}")
              debug.append(f"  v['name']: {v['name']}")
              wcd = ET.Element('def:WhereClauseDef')
              wcd.set('OID',where_clause_oid(v))
              debug.append(f"    wcd oid {where_clause_oid(v)}")
              wcd.append(range_check(v['testcd'], 'EQ', 'Soft', testcd_oid))
              wcds[key] = wcd

            # wcds.append(wcd)
    # return wcds
    return list(wcds.values())

ODM_XML = Path.cwd() / "data" / "odm" / "odm.xml"
# ODM_XLS = Path.cwd() / "data" / "define" / "stylesheets" / "define2-1.xsl"
# ODM_HTML = Path.cwd() / "data" / "odm" / "odm.html"
# DEFINE_XML = Path('/Users/johannes/dev/python/github/study_service/uploads/define.xml')

def generate_odm():
  try:
    study_info = get_study_info()
    # debug.append(f"study_info {study_info}")
    # domains = get_domains_and_variables(study_info['uuid'])
    activities = get_activities(study_info['uuid'])
    activities = activities[0:8]
    for activity in activities:
       debug.append(activity)
 
# {'act': {'instanceType': 'Activity', 'name': 'CT scan', 'description': '', 'label': 'CT scan', 'id': 'Activity_18', 'uuid': '67671a28-d1b8-4b27-835c-97c11e67111a'}, 'bc': None, 'bcp': None, 'dc': None, 'sai': {'instanceType': 'ScheduledActivityInstance', 'name': 'SCREEN1', 'description': '-', 'label': 'Screen One', 'id': 'ScheduledActivityInstance_9', 'uuid': 'b2fd136f-0c70-4d4e-9ae3-039f099a2d06'}, 'sd': {'instanceType': 'StudyDesign', 'name': 'Study Design 1', 'description': 'The main design for the study', 'id': 'StudyDesign_1', 'label': '', 'uuid': '47343f4e-c9e0-44cf-b5de-09c3baf455d5', 'rationale': 'The discontinuation rate associated with this oral dosing regimen was 58.6% in previous studies, and alternative clinical strategies have been sought to improve tolerance for the compound. To that end, development of a Transdermal Therapeutic System (TTS) has been initiated.'}}
# {'act': {'instanceType': 'Activity', 'name': 'Patient randomised', 'description': '', 'label': 'Patient randomised', 'id': 'Activity_12', 'uuid': 'ae8b6213-8726-4f31-9294-ddee8d9b2850'}, 'bc': None, 'bcp': None, 'dc': None, 'sai': {'instanceType': 'ScheduledActivityInstance', 'name': 'DOSE', 'description': '-', 'label': 'Dose', 'id': 'ScheduledActivityInstance_11', 'uuid': '45bb7fd7-1023-4ef4-8b4a-b02ecd682775'}, 'sd': {'instanceType': 'StudyDesign', 'name': 'Study Design 1', 'description': 'The main design for the study', 'id': 'StudyDesign_1', 'label': '', 'uuid': '47343f4e-c9e0-44cf-b5de-09c3baf455d5', 'rationale': 'The discontinuation rate associated with this oral dosing regimen was 58.6% in previous studies, and alternative clinical strategies have been sought to improve tolerance for the compound. To that end, development of a Transdermal Therapeutic System (TTS) has been initiated.'}}



    root = ET.Element('ODM')
    odm_properties(root)
    study = set_study_info(study_name=study_info['study_name'])
    # Study -------->
    study.append(set_globalvariables(study_name=study_info['study_name'], study_description=study_info['rationale'], protocol_name=study_info['protocol_name']))

    # MetadataVersion -------->
    metadata = metadata_version(oid=study_info['uuid'], name=study_info['study_name'],description="This is some kind of description")

    # Protocol -------->
    metadata.append(protocol(study_info, activities))

    # StudyEventDef -------->
    seds = studyeventdefs(study_info, activities)
    for sed in seds:
      metadata.append(sed)

    # FormDef -------->
    fds = formdefs(study_info, activities)
    for fd in fds:
      metadata.append(fd)

    # ItemGroupDef -------->
    igds = form_itemgroupdefs(activities)
    for igd in igds:
      metadata.append(igd)

    # ItemDef -------->
    igds = form_itemdefs(activities)
    for igd in igds:
      metadata.append(igd)
 
 
    # # MetadataVersion <--------
    # # Study <--------
    study.append(metadata)
    root.append(study)

    debug.append(root)

    write_tmp("odm-debug.txt",debug)
    return root

  except Exception as e:
    write_tmp("odm-debug.txt",debug)
    print("Error",e)
    print(traceback.format_exc())
    debug.append(f"Error: {e}")

def save_xml(xml):
    print("Saving xml...", ODM_XML)
    tree = ET.ElementTree(xml)
    # ET.indent(tree, space="\t", level=0)
    ET.indent(tree, space="   ", level=0)
    tree.write(ODM_XML, encoding="utf-8")
    # add stylesheet
    with open(ODM_XML,'r') as f:
      lines = f.readlines()
    lines.insert(0,xml_header) 
    with open(ODM_XML,'w') as f:
      for line in lines:
         f.write(line)
      # lines = f.readlines()

def xml_to_html():
    print("Saving html...", ODM_HTML)
    dom = etree.parse(ODM_XML)
    xslt =etree.parse(ODM_XLS)
    transform = etree.XSLT(xslt)
    newdom = transform(dom)
    raw_html = etree.tostring(newdom, pretty_print=True)

    soup = bs(raw_html, features="lxml")
    prettyHTML = soup.prettify()


    with open(ODM_HTML,'w') as f:
          # f.write(str(html))
          f.write(prettyHTML)

def main():
    xml = generate_odm()
    if xml:
      print("done with xml, time to save xml")
      save_xml(xml)
      print("done with xml")
#   xml_to_html()

def check_odm():
    from pprint import pprint
    schema_file = '/Users/johannes/Library/CloudStorage/OneDrive-data4knowledge/shared_mac/standards/define-xml/DefineV217_0/schema/cdisc-define-2.1/define2-1-0.xsd'
    schema = xmlschema.XMLSchema(schema_file)
    odm_file = ODM_XML
    a = schema.to_dict(odm_file)
    # pprint(schema.to_dict(odm_file))

if __name__ == "__main__":
    # check_crm_links()
    _add_missing_links_to_crm()
    main()
    # check_odm()
