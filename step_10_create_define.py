import json
import copy
from pathlib import Path
from d4kms_service import Neo4jConnection
from model.configuration import Configuration, ConfigurationNode
from model.base_node import BaseNode
from utility.debug import write_debug, write_tmp, write_tmp_json, write_define_json, write_define_xml, write_define_xml2
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
      query = """
        MATCH (crm:CRMNode {uri:'%s'})
        MATCH (v:Variable {name:'%s'})
        with crm, v
        MERGE (v)-[r:IS_A_REL]->(crm)
        set r.fake_relationship = "yes"
        return "done" as done
      """ % (uri,var)
      # print("DS crm query",query)
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


# class Define(BaseNode):
#   name: str = ""
#   study_products_bcs: List[str]= []
#   disposition: List[str]= []
#   demography: List[str]= []

debug = []

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

def _study(oid= 'tbd', study_name = 'tbd', description = 'tbd', protocol_name = 'tbd'):
  study = ET.Element('Study')
  study.set('OID',study_name)
  # study.set('StudyName', study_name)
  # study.set('StudyDescription', description)
  # study.set('ProtocolName', protocol_name)
  return study

# ISSUE: Hardcoded
def globalvariables():
  global_variables = ET.Element('GlobalVariables')
  element = ET.Element('StudyName')
  element.text = 'Study name'
  global_variables.append(element)
  element = ET.Element('StudyDescription')
  element.text = 'Study Description'
  global_variables.append(element)
  element = ET.Element('ProtocolName')
  element.text = 'Protocol Name'
  global_variables.append(element)
  return global_variables

# ISSUE: Hardcoded
def standards():
  standards = ET.Element('def:Standards')

  standard1 = ET.Element('def:Standard')
  standard1.set("OID", "STD.1")
  standard1.set("Name", "SDTMIG")
  standard1.set("Type", "IG")
  standard1.set("Version", "3.4")
  standard1.set("Status", "Final")
  standards.append(standard1)

  return standards

def metadata_version(oid = 'tbd', name = 'tbd', description = 'tbd'):
  metadata = ET.Element('MetaDataVersion')
  metadata.set("OID", oid)
  metadata.set("Name", name)
  metadata.set("Description", description)
  metadata.set("def:DefineVersion", "2.1.7")
  return metadata

def get_study_design_uuid():
    db = Neo4jConnection()
    with db.session() as session:
      query = """
        MATCH (sd:StudyDesign) RETURN sd.uuid as uuid
      """
      results = session.run(query)
      data = [r.data() for r in results][0]['uuid']
    db.close()
    return data

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
    db.close()


def get_domains(uuid):
    db = Neo4jConnection()
    with db.session() as session:
      query = """
        MATCH (sd:StudyDesign {uuid: '%s'})-[]->(d:Domain) RETURN d
        ORDER BY d.name
      """ % (uuid)
      # print("domains query", query)
      results = session.run(query)
      return [r['d'] for r in results]
    db.close()

def get_variables(uuid):
    db = Neo4jConnection()
    with db.session() as session:
      query = """
        MATCH (d:Domain {uuid: '%s'})-[]->(v:Variable) RETURN v
        ORDER BY v.name
      """ % (uuid)
      # print("variables query", query)
      results = session.run(query)
      all_variables = [r['v'] for r in results]
      required_variables = [v for v in all_variables if v['core'] == 'Req']

      # CRM linked vars
      query = """
        MATCH (d:Domain {uuid: '%s'})-[]->(v:Variable)-[:IS_A_REL]->(:CRMNode) RETURN v
        ORDER BY v.name
      """ % (uuid)
      # print("variables query", query)
      results = session.run(query)
      vars_in_use = [r['v'] for r in results]
    db.close()
    return vars_in_use


def get_define_first(domain_uuid):
    db = Neo4jConnection()
    with db.session() as session:
      query = """
        MATCH (sd:StudyDesign)-[:DOMAIN_REL]->(domain:Domain {uuid:'%s'})
        MATCH (sd)<-[:STUDY_DESIGNS_REL]-(sv:StudyVersion)
        MATCH (sv)-[:STUDY_IDENTIFIERS_REL]->(si:StudyIdentifier)-[:STUDY_IDENTIFIER_SCOPE_REL]->(:Organization {name:'Eli Lilly'})
        WITH si, domain
        MATCH (domain)-[:USING_BC_REL]-(bc:BiomedicalConcept)-[:PROPERTIES_REL]->(bcp:BiomedicalConceptProperty)
        MATCH (bc)-[:CODE_REL]-(:AliasCode)-[:STANDARD_CODE_REL]->(cd:Code)
        WHERE EXISTS {
            (bcp)<-[:PROPERTIES_REL]->(dc:DataContract)
        }
        WITH bc, cd, bcp, domain
        MATCH (bcp)-[:IS_A_REL]->(crm:CRMNode)<-[:IS_A_REL]-(var:Variable)<-[:VARIABLE_REL]-(domain)
        MATCH (bcp)-[:RESPONSE_CODES_REL]->(rc:ResponseCode)-[:CODE_REL]->(c:Code)
        WITH bc, cd, bcp, crm, var, c
        ORDER By bc.name, cd.decode, bcp.name, c.decode
        WITH bc, cd, bcp, crm, var, collect({code:c.code,decode:c.decode}) as decodes
        return distinct 
        bc.name as bc,
        // bc.uuid as bc_uuid,
        cd.decode as testcd,
        bcp.name as bcp,
        crm.datatype as datatype,
        var.uuid as uuid,
        var.label as label,
        var.name as name,
        var.core as core,
        var.ordinal as ordinal,
        decodes as decodes
        order by ordinal
      """ % (domain_uuid)
      # limit 100
      # print("define query", query)
      # debug.append("define query")
      # debug.append(query)
      results = session.run(query)
      # return [r.data() for r in results]
      data = [r for r in results.data()]
      for d in data:
         debug.append(d)
    db.close()
    return data
   
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
    item = {}
    for k,v in d._properties.items():
        item[k] = v
    all_variables = get_variables(d['uuid'])
    debug.append(f"domain {d['name']}")
    define_metadata = get_define_first(d['uuid'])
    # vlm = define_metadata
    item['vlm'] = list(define_metadata)
    print(d['name'],"len(define_metadata)", len(define_metadata))
    unique_vars = get_unique_vars(copy.deepcopy(define_metadata))
    # print(unique_vars)

    item['variables'] = unique_vars
    domains.append(item)

  return domains

def translated_text(language, text):
    translated_text = ET.Element('TranslatedText')
    translated_text.set('xml:lang',language)
    translated_text.text = text
    return translated_text

def description(language, text_str):
    description = ET.Element('Description')
    description.append(translated_text(language, text_str))
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
        for v in d['variables']:
          # debug.append(v)
          idf = ET.Element('ItemDef')
          idf.set('OID', v['uuid'])
          idf.set('Name', v['name'])
          datatype = DATATYPES[v['datatype']]
          idf.set('DataType', datatype)
          idf.set('Length', '8')
          idf.set('SASFieldName', v['name'])
          idf.append(description('en',v['label']))
          idf.append(origin('Collected','Sponsor'))
          idfs.append(idf)
    return idfs

def value_list_defs(domains):
    vlds = []
    for d in domains:
        debug.append(f"\ndomain: {d['name']}")
        for v in d['variables']:
          vlms  = [x for x in d['vlm'] if x['uuid'] == v['uuid']]
          if vlms:
            debug.append(f"len(vlm): {len(vlms)}")
            vld = ET.Element('def:ValueListDef')
            vld.set('OID', f"VL.{v['name']}.{v['uuid']}")
            item_refs = []
            i = 1
            for vlm in vlms:
              item_ref = ET.Element('ItemRef')
              item_ref.set('ItemOID', f"{i}.{vlm['uuid']}")
              item_ref.set('OrderNumber', str(i))
              item_ref.set('Mandatory', 'No')
              wcd = ET.Element("def:WhereClauseRef")
              wcd.set('WhereClauseOID', where_clause_oid(d['name'], v['name'], v['testcd'])) 
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

def where_clause_oid(domain, variable, test):
    return f"WC.{domain}.{variable}.{pretty_string(test)}"

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
          debug.append(f"v['name']: {v['name']}")
          debug.append(f"len(vlms): {len(vlms)}")
          for vlm in vlms:
            # debug.append(vlm)
            wcd = ET.Element('def:WhereClauseDef')
            wcd.set('OID',where_clause_oid(d['name'], v['name'], v['testcd']))
            wcd.append(range_check(vlm['decodes'], 'IN', 'Soft', v['uuid']))
          # debug.append(wcd)
          wcds.append(wcd)
    return wcds

DEFINE_JSON = Path.cwd() / "tmp" / "define.json"
DEFINE_XML = Path.cwd() / "tmp" / "define.xml"

def main():
  define = {}
  root = ET.Element('ODM')
  odm_properties(root)
  study = _study()
  # Study -------->
  study.append(globalvariables())

  # MetadataVersion -------->
  metadata = metadata_version()
  metadata.append(standards())
  
  sd_uuid = get_study_design_uuid()
  domains = get_domains_and_variables(sd_uuid)

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


  # # CodeList
  # # MethodDef
  # # def:CommentDef
  # # def:leaf

  # MetadataVersion <--------
  # Study <--------
  study.append(metadata)
  root.append(study)

  write_tmp("define-debug.txt",debug)

  try:
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
     print("Error",e)
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
