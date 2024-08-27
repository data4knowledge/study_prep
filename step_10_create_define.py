import json
import copy
import traceback
from pathlib import Path
from model.configuration import Configuration, ConfigurationNode
from d4kms_service import Neo4jConnection
from model.base_node import BaseNode
from utility.debug import write_debug, write_tmp, write_tmp_json, write_define_json, write_define_xml, write_define_xml2
from utility.define_query import define_vlm_query, crm_link_query, _add_missing_links_to_crm_query, study_info_query, domains_query, domain_variables_query, variables_crm_link_query, define_codelist_query, define_test_codes_query, find_ct_query
from datetime import datetime
import xmlschema
import xml.etree.ElementTree as ET

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


class Define:
    """ Generate a Define-XML v2.1 file from the SDTM Metadata Worksheet Example Excel file """
    def __init__(self, excel_file, define_file, is_check=False):
        """
        :param excel_file: str - the path and filename for the SDTM metadata worksheet excel input file
        :param define_file: str - the path and filename for the Define-XML v2.0 file to be generated
        :param is_check: boolean - flag that indicates if the conformance checks should be executed
        """
        # self.excel_file = excel_file
        self.define_file = define_file
        # self.is_check_conformance = is_check
        # self._check_file_existence()
        # self.workbook = load_workbook(filename=self.excel_file, read_only=True, data_only=True)
        self.lang = "en"
        self.acrf = "LF.acrf"
        self.define_objects = {}



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

# class Define(BaseNode):
#   name: str = ""
#   study_products_bcs: List[str]= []
#   disposition: List[str]= []
#   demography: List[str]= []

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
      return [r['d'] for r in results]
    db.close()

def get_variables(uuid):
    db = Neo4jConnection()
    with db.session() as session:
      query = domain_variables_query(uuid)
      # print("variables query", query)
      results = session.run(query)
      # all_variables = [r['v'] for r in results]
      all_variables = [r['v'] for r in results.data()]
      required_variables = [v for v in all_variables if v['core'] == 'Req']

      # CRM linked vars
      query = variables_crm_link_query(uuid)
      results = session.run(query)
      vars_in_use = [r['v'] for r in results.data()]
    db.close()
    # return vars_in_use
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
      # debug.append("codelist query")
      # debug.append(query)
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
      # debug.append("test_codes query")
      # debug.append(query)
      results = session.run(query)
      data = [r for r in results.data()]
      debug.append("test_codes--->")
      for d in data:
         debug.append(d)
      debug.append("test_codes<---")
    db.close()
    return data

def pretty_string(text):
   return text.replace(' ','_')

def odm_properties(root):
  # now = datetime.now().replace(tzinfo=datetime.timezone.utc).isoformat()
  now = datetime.now().isoformat()
  root.set('xmlns',"http://www.cdisc.org/ns/odm/v1.3")
  root.set('xmlns:xlink',"http://www.w3.org/1999/xlink")
  root.set('xmlns:def',"http://www.cdisc.org/ns/def/v2.1")
  root.set('ODMVersion',"1.3.2")
  root.set('FileOID',"www.cdisc.org/StudyCDISC01_1/1/Define-XML_2.1.0")
  root.set('FileType',"Snapshot")
  root.set('CreationDateTime',now)
  root.set('Originator',"Study Service")
  root.set('SourceSystem',"Study service")
  root.set('SourceSystemVersion',"Alpha1")
  root.set('def:Context',"Other")

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

def metadata_version(oid = 'Not set', name = 'Not set', description = 'Not set'):
  metadata = ET.Element('MetaDataVersion')
  metadata.set("OID", oid)
  metadata.set("Name", name)
  metadata.set("Description", description)
  metadata.set("def:DefineVersion", "2.1.7")
  return metadata

# {'sd': {'instanceType': 'StudyDesign', 'name': 'Study Design 1', 'description': 'The main design for the study', 'id': 'StudyDesign_1', 'label': '', 'uuid': '39309ff3-546c-4439-aa6f-74f16ad36f8f', 'rationale': 'The discontinuation rate associated with this oral dosing regimen was 58.6% in previous studies, and alternative clinical strategies have been sought to improve tolerance for the compound. To that end, development of a Transdermal Therapeutic System (TTS) has been initiated.'},
#  'si': {'instanceType': 'StudyIdentifier', 'id': 'StudyIdentifier_1', 'studyIdentifier': 'H2Q-MC-LZZT', 'uuid': '224be614-0648-440e-b8ae-2cb0c642c1f1'},
#  'sv': {'versionIdentifier': '2', 'instanceType': 'StudyVersion', 'id': 'StudyVersion_1', 'uuid': 'f347c6df-94ea-406e-a5df-c3e6d6942dbd', 'rationale': 'The discontinuation rate associated with this oral dosing regimen was 58.6% in previous studies, and alternative clinical strategies have been sought to improve tolerance for the compound. To that end, development of a Transdermal Therapeutic System (TTS) has been initiated.'}}

def get_unique_vars(vars):
  unique_vars = []
  for v in vars:
      v.pop('bc')
      if 'bc_uuid' in v:
        v.pop('bc_uuid')
      v.pop('decodes')
      unique_vars.append(v)
  unique_vars = list({v['uuid']:v for v in unique_vars}.values())
  return unique_vars


def get_domains_and_variables(uuid):
  domains = []
  raw_domains = get_domains(uuid)
  for d in raw_domains:
    debug.append(f"domain {d['name']}")
    item = {}
    for k,v in d._properties.items():
        item[k] = v
    all_variables = get_variables(d['uuid'])
    # for v in all_variables:
    #    debug.append(v)
    codelist_metadata = get_define_codelist(d['uuid'])
    item['codelist'] = codelist_metadata
    goc = next((x for x,y in DOMAIN_CLASS.items() if d['name'] in y), "Fix")
    if goc in ['FINDINGS','FINDINGS ABOUT']:
      test_codes = get_define_test_codes(d['uuid'])
      for k in test_codes:
         debug.append(f"tc {k}")
      item['test_codes'] = test_codes
    vlm_metadata = get_define_vlm(d['uuid'])
    item['vlm'] = vlm_metadata
    # vlm = vlm_metadata
    for m in vlm_metadata:
      # debug.append(f"check var {m['name']}")
      vs = [v for v in all_variables if v['name'] == m['name']]

      # debug.append(f"   {m}")
      # for v in vs:
      #   # debug.append(f"   {v['name']}")
      #   debug.append(f"   {v}")
          
      #  if next((v for v in all_variables if v['name'] == m['name']), None):
      #     debug.append("yes, a hit")
    print(d['name'],"len(vlm_metadata)", len(vlm_metadata))
    unique_vars = get_unique_vars(copy.deepcopy(vlm_metadata))
    # print(unique_vars)

    # item['variables'] = unique_vars
    item['variables'] = all_variables
    domains.append(item)

  return domains

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
      ref.set('ItemOID', v['uuid'])
      mandatory = 'No' if v['core'] == 'Perm' else 'Yes'
      ref.set('Mandatory', mandatory)
      order = int(v['ordinal'])
      ref.set('OrderNumber', str(order))
      # ref.set('KeySequence', "1")
      variable_refs.append(ref)
    return variable_refs

def item_group_defs(domains):
    igds = []
    for d in domains:
        igd = ET.Element('ItemGroupDef')
        igd.set('OID', d['uuid'])
        igd.set('Domain', d['name'])
        igd.set('Name', d['name'])
        igd.set('Repeating', 'No')
        igd.set('IsReferenceData', 'No')
        igd.set('SASDatasetName', d['name'])
        igd.set('def:Structure', 'tbc')
        igd.set('Purpose', 'Tabulation')
        igd.set('def:StandardOID', 'STD.1')
        igd.set('def:ArchiveLocationID', f"LI.{d['name']}")
        igd.append(description('en',d['label']))
        # ISSUE/Question: Why does the order matter? Had to move refs after description
        refs = set_variable_refs(d['variables'])
        for ref in refs:
          igd.append(ref)
        goc = next((x for x,y in DOMAIN_CLASS.items() if d['name'] in y), "Fix")
        ET.SubElement(igd,'def:Class', {'Name': goc})
        # ET.SubElement(igd,'def:Class').text = goc
        # goc_e.text = goc
        igd.append(leaf(f"LI.{d['name']}", d['name'].lower()+".xpt", d['name'].lower()+".xpt"))
        igds.append(igd)
    return igds

def item_defs(domains):
    idfs = []
    for d in domains:
        for item in d['variables']:
          # debug.append(f"2 item {item}")
          idf = ET.Element('ItemDef')
          idf.set('OID', item['uuid'])
          idf.set('Name', item['name'])
          datatype = DATATYPES[item['datatype']] if 'datatype' in item else ""
          if datatype == "":
            # NOTE: Using SDTM datatype. Not always correct e.g. VISITNUM
            datatype = DATATYPES[item['data_type']]
          idf.set('DataType', datatype)
          idf.set('Length', '8')
          idf.set('SASFieldName', item['name'])
          idf.append(description('en',item['label']))
          idf.append(origin('Collected','Sponsor'))
          if next((x for x in d['codelist'] if x['uuid'] == item['uuid']), None):
             print("found codelist", d['name'], item['name'])
             cl_ref = ET.Element('CodeListRef')
             cl_ref.set('CodeListOID', codelist_oid(item['name'], item['uuid']))
             idf.append(cl_ref)
          if next((x for x in d['vlm'] if x['uuid'] == item['uuid']), None):
            vl_ref = ET.Element('def:ValueListRef')
            vl_ref.set('ValueListOID', value_list_oid(item['name'], item['uuid']))
            idf.append(vl_ref)
          # <def:ValueListRef ValueListOID="VL.LB.LBORRES"/>

          idfs.append(idf)
    return idfs

def codelist_oid(variable, uuid):
    return f"CL.{pretty_string(variable)}.{uuid}"
    # return f"CL.{variable}"

def codelist_test_oid(domain, variable):
    return f"CL.{domain}.{pretty_string(variable)}"
    # return f"CL.{variable}"

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
    # debug.append(f"code {code} short {short} long {long} context {context}")
    e = ET.Element('CodeListItem')
    e.set('CodedValue', short)
    d = ET.SubElement(e,'Decode')
    d.append(translated_text(long))
    if code:
      e.append(alias(context, code))
    return e

def codelist_name(item):
   return f"CL {item['name']} {item['testcd']} ({item['bc']}"

def codelist_defs(domains):
    codelists = []
    for d in domains:
        for item in d['codelist']:
          cl = ET.Element('CodeList')
          cl.set('OID', codelist_oid(item['name'], item['uuid']))
          # cl.set('OID', codelist_oid(item['testcd'], item['uuid']))
          cl.set('Name', codelist_name(item))
          cl.set('def:StandardOID', "STD.2")
          datatype = DATATYPES[item['datatype']] if 'datatype' in item else ""
          cl.set('DataType', datatype)
          codes = [x['code'] for x in item['decodes']]
          clis = get_concept_info(codes)
          for cli in clis:
            # NOTE: Need to care for enumerated item?
            # cl.append(enumerated_item(x['code'], "nci:ExtCodeID",x['decode']))
            cl.append(codelist_item(cli['code'], cli['notation'], cli['pref_label'], "nci:ExtCodeID"))
          codelists.append(cl)
    return codelists

def test_codelist_name(item):
   return f"CL {item['domain']} {item['domain']+'TESTCD'}"

def test_codes_defs(domains):
    test_codes = []
    for d in domains:
        debug.append(f"-1-1-1 {d['name']}")
        if 'test_codes' in d:
          for item in d['test_codes']:
            cl = ET.Element('CodeList')
            cl.set('OID', codelist_test_oid(item['domain'], item['domain']+'TESTCD'))
            cl.set('Name', test_codelist_name(item))
            cl.set('def:StandardOID', "STD.1")
            cl.set('DataType', "text")
            debug.append(f"1 codelist {item}")
            for test in item['test_codes']:
              # debug.append(f"testcodes {test}")
              # cl.append(enumerated_item(x['code'], "nci:ExtCodeID",x['decode']))
              cl.append(codelist_item(test['code'], test['testcd'], test['test'], "nci:ExtCodeID"))
            test_codes.append(cl)
        debug.append(f"len(test_codes) {len(test_codes)}")
    return test_codes

def value_list_oid(variable, uuid):
    return f"VL.{variable}.{uuid}"

def value_list_defs(domains):
    vlds = []
    for d in domains:
        # debug.append(f"\ndomain: {d['name']}")
        goc = next((x for x,y in DOMAIN_CLASS.items() if d['name'] in y), "Fix")
        if goc in ['FINDINGS','FINDINGS ABOUT']:
          for v in d['variables']:
            vlms  = [x for x in d['vlm'] if x['uuid'] == v['uuid']]
            if vlms:
              # NOTE: Make one for all items for the variable
              # NOTE: Make one per test code (VLM)
              # debug.append(f"\nVariable: {v['name']}")
              # debug.append(f"len(vlm): {len(vlms)}")
              vld = ET.Element('def:ValueListDef')
              vld.set('OID', value_list_oid(v['name'], v['uuid']))
              item_refs = []
              i = 1
              for vlm in vlms:
                # debug.append(f"vlm: {vlm}")
                item_ref = ET.Element('ItemRef')
                item_ref.set('ItemOID', f"{i}.{vlm['uuid']}")
                item_ref.set('OrderNumber', str(i))
                item_ref.set('Mandatory', 'No')
                wcd = ET.Element("def:WhereClauseRef")
                wcd.set('WhereClauseOID', where_clause_oid(v['uuid'],d['name'], vlm['name'], vlm['testcd'])) 
                item_ref.append(wcd)
                # TODO: WhereClauseRef
                # item_ref.set('def:WhereClauseRef'], {)
                #   "def:WhereClauseRef":
                #         {
                #             "@WhereClauseOID": "FIX"
                #         }
                # }
                # item_refs.append(ref)
                i += 1
                item_refs.append(item_ref)
                # vld.append(item_ref)
              # debug.append(ET.dump(vld))
              for ref in item_refs:
                vld.append(ref)
              vlds.append(vld)
    return vlds

def where_clause_oid(var_uuid, domain, variable, test):
    return f"WC.{domain}.{variable}.{pretty_string(test)}.{var_uuid}"

def range_check(decodes,comparator, soft_hard, item_oid):
    range_check = ET.Element('RangeCheck')
    range_check.set('Comparator', comparator)
    range_check.set('SoftHard', soft_hard)
    range_check.set('def:ItemOID', item_oid)
    for decode in decodes:
      check_value = ET.Element('CheckValue')
      check_value.text = decode['decode']
      range_check.append(check_value)
    return range_check

def where_clause_defs(domains):
    wcds = []
    for d in domains:
        debug.append(f"\ndomain: {d['name']}")
        for v in d['vlm']:
          vlms  = [x for x in d['vlm'] if x['uuid'] == v['uuid']]
          # debug.append(f"v['name']: {v['name']}")
          # debug.append(f"len(vlms): {len(vlms)}")
          for vlm in vlms:
            # debug.append(vlm)
            wcd = ET.Element('def:WhereClauseDef')
            wcd.set('OID',where_clause_oid(v['uuid'],d['name'], v['name'], v['testcd']))
            wcd.append(range_check(vlm['decodes'], 'IN', 'Soft', v['uuid']))
          # debug.append(wcd)
          wcds.append(wcd)
    return wcds

DEFINE_JSON = Path.cwd() / "tmp" / "define.json"
DEFINE_XML = Path.cwd() / "tmp" / "define.xml"
# DEFINE_XML = Path('/Users/johannes/dev/python/github/study_service/uploads/define.xml')

def main():
  try:
    study_info = get_study_info()
    domains = get_domains_and_variables(study_info['uuid'])
    debug.append(f"study_info {study_info}")

    define = {}
    root = ET.Element('ODM')
    odm_properties(root)
    study = set_study_info(study_name=study_info['study_name'])
    # Study -------->
    study.append(set_globalvariables(study_name=study_info['study_name'], study_description=study_info['rationale'], protocol_name=study_info['protocol_name']))

    # MetadataVersion -------->
    metadata = metadata_version(oid=study_info['uuid'], name=study_info['study_name'],description="This is some kind of description")
    metadata.append(standards())
    
    # def:ValueListDef
    vlds = value_list_defs(domains)
    for vld in vlds:
      metadata.append(vld)

    # def:WhereClauseDef
    wcds = where_clause_defs(domains)
    for wcd in wcds:  
      metadata.append(wcd)

    # ItemGroupDef
    igds = item_group_defs(domains)
    for igd in igds:
      metadata.append(igd)

    # ItemDef
    idfs = item_defs(domains)
    for idf in idfs:
      metadata.append(idf)

    # CodeList
    codelists = codelist_defs(domains)
    for codelist in codelists:
      metadata.append(codelist)
    test_codes = test_codes_defs(domains)
    for codelist in test_codes:
      metadata.append(codelist)

    # def:CommentDef
    comments = comment_defs()
    for comment in comments:
      metadata.append(comment)


    # # MethodDef
    # # def:leaf

    # MetadataVersion <--------
    # Study <--------
    study.append(metadata)
    root.append(study)

    write_tmp("define-debug.txt",debug+debug)

    # debug.append(define)
    write_tmp_json("define-debug",define)
    json_data = write_define_json(DEFINE_JSON,define)
    tree = ET.ElementTree(root)
    # ET.indent(tree, space="\t", level=0)
    ET.indent(tree, space="   ", level=0)
    tree.write(DEFINE_XML, encoding="utf-8")
    # add stylesheet
    with open(DEFINE_XML,'r') as f:
      lines = f.readlines()
    stuff = """<?xml version="1.0" encoding="UTF-8"?>
<?xml-stylesheet type="text/xsl" href="stylesheets/define2-1.xsl"?>
"""
    lines.insert(0,stuff) 
    with open(DEFINE_XML,'w') as f:
      for line in lines:
         f.write(line)
      # lines = f.readlines()

    # write_define_xml(DEFINE_XML,define)
    # write_define_xml(DEFINE_XML,json_data)
    # write_define_xml1(DEFINE_XML,define)
    # write_define_xml2(DEFINE_XML,json_data)
  except Exception as e:
    write_tmp("define-debug.txt",debug)
    print("Error",e)
    print(traceback.format_exc())
    debug.append(f"Error: {e}")

def check_define():
    from pprint import pprint
    schema_file = '/Users/johannes/Library/CloudStorage/OneDrive-data4knowledge/shared_mac/standards/define-xml/DefineV217_0/schema/cdisc-define-2.1/define2-1-0.xsd'
    schema = xmlschema.XMLSchema(schema_file)
    define_file = DEFINE_XML
    a = schema.to_dict(define_file)
    # pprint(schema.to_dict(define_file))

if __name__ == "__main__":
    # check_crm_links()
    _add_missing_links_to_crm()
    main()
    # check_define()
