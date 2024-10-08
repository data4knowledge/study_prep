import json
from pathlib import Path
from d4kms_service import Neo4jConnection
from model.configuration import Configuration, ConfigurationNode
from model.base_node import BaseNode
from utility.debug import write_debug, write_tmp, write_tmp_json, write_define_json, write_define_xml, write_define_xml2
import xmlschema
import xml.etree.ElementTree as ET

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

# ISSUE: Should be in DB
DOMAIN_CLASS = {
  'Events'          :['AE', 'BE', 'CE', 'DS', 'DV', 'HO', 'MH'],
  'Findings'        :['BS', 'CP', 'CV', 'DA', 'DD', 'EG', 'FT', 'GF', 'IE', 'IS', 'LB', 'MB', 'MI', 'MK', 'MS', 'NV', 'OE', 'PC', 'PE', 'PP', 'QS', 'RE', 'RP', 'RS', 'SC', 'SS', 'TR', 'TU', 'UR', 'VS'],
  'Findings About'  :['FA', 'SR'],
  'Interventions'   :['AG', 'CM', 'EC', 'EX', 'ML', 'PR', 'SU'],
  'Relationship'    :['RELREC', 'RELSPEC', 'RELSUB', 'SUPPQUAL'],
  'Special-Purpose' :['CO', 'DM', 'SE', 'SM', 'SV'],
  'Study Reference' :['OI'],
  'Trial Design'    :['TA', 'TD', 'TE', 'TI', 'TM', 'TS', 'TV'],
}

# ISSUE: Fix proper links when loading
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


# class Define(BaseNode):
#   name: str = ""
#   study_products_bcs: List[str]= []
#   disposition: List[str]= []
#   demography: List[str]= []

debug = []

def odm_properties(odm_file):
  odm_file['xmlns'] = "http://www.cdisc.org/ns/odm/v1.3" ,
  odm_file['xmlns:xlink'] = "http://www.w3.org/1999/xlink",
  odm_file['xmlns:def'] = "http://www.cdisc.org/ns/def/v2.1",
  odm_file['ODMVersion'] = "1.3.2",
  odm_file['FileOID'] = "www.cdisc.org/StudyCDISC01_1/1/Define-XML_2.1.0",
  odm_file['FileType'] = "Snapshot",
  odm_file['CreationDateTime'] = "",
  odm_file['Originator'] = "Study Service",
  odm_file['SourceSystem'] = "Study service",
  odm_file['SourceSystemVersion'] = "Alpha1",
  odm_file['def:Context'] = "USDM",

def odm_properties():
  properties = {
    'xmlns':"http://www.cdisc.org/ns/odm/v1.3" ,
    'xmlns:xlink':"http://www.w3.org/1999/xlink",
    'xmlns:def':"http://www.cdisc.org/ns/def/v2.1",
    'ODMVersion':"1.3.2",
    'FileOID':"www.cdisc.org/StudyCDISC01_1/1/Define-XML_2.1.0",
    'FileType':"Snapshot",
    'CreationDateTime':"",
    'Originator':"Study Service",
    'SourceSystem':"Study service",
    'SourceSystemVersion':"Alpha1",
    'def:Context':"USDM",
  }
  return properties

def study(oid= 'tbd', study_name = 'tbd', description = 'tbd', protocol_name = 'tbd'):
  study = {
    '@OID': oid,
    'StudyName': study_name,
    'StudyDescription': description,
    'ProtocolName': protocol_name,

  }
  return study

def metadata_version(oid = 'tbd', name = 'tbd', description = 'tbd'):
  metadata = {
    "@OID": oid,
    "@Name": name,
    "@Description": description,
    "@def:DefineVersion": "2.1.7",
    "def:Standards": [
       {
        "@OID": "STD.1",
        "@Name": "SDTMIG",
        "@Type": "IG",
        "@Version": "3.4",
        "@Status": "Final",          
       }
    ]
  }
      # "@def:CommentOID": "COM.STD1"
  return metadata

def get_study_design_uuid():
    db = Neo4jConnection()
    with db.session() as session:
      query = """
        MATCH (sd:StudyDesign) RETURN sd.uuid as uuid
      """
      results = session.run(query)
      return [r.data() for r in results][0]['uuid']

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
        WHERE EXISTS {
            (bcp)<-[:PROPERTIES_REL]->(dc:DataContract)
        }
        WITH bc, bcp, domain
        MATCH (bcp)-[:IS_A_REL]->(crm:CRMNode)<-[:IS_A_REL]-(var:Variable)<-[:VARIABLE_REL]-(domain)
        MATCH (bcp)-[:RESPONSE_CODES_REL]->(rc:ResponseCode)-[:CODE_REL]->(c:Code)
        WITH bc, bcp, crm, var, c
        ORDER By bc.name, bcp.name, c.decode
        WITH bc, bcp, crm, var, collect({code:c.code,decode:c.decode}) as decodes
        return distinct 
        bc.name as bc,
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
      results = session.run(query)
      # return [r.data() for r in results]
      return [r for r in results.data()]
   


def get_domains_and_variables(uuid):
   domains = []
   raw_domains = get_domains(uuid)
   for d in raw_domains:
      item = {}
      for k,v in d._properties.items():
         item[k] = v
      # print("domain",d['name'])
      variables = get_variables(d['uuid'])
      # print("len(variables)", len(variables))
      define_metadata = get_define_first(d['uuid'])
      print(d['name'],"len(define_metadata)", len(define_metadata))
      unique_vars = []
      for v in define_metadata:
          v.pop('bc')
          v.pop('decodes')
          unique_vars.append(v)
      unique_vars = list({v['uuid']:v for v in unique_vars}.values())
      print(unique_vars)
      vlm = define_metadata

      item['vlm'] = vlm
      item['variables'] = unique_vars
      domains.append(item)


   return domains

def set_variable_refs(variables):
    variable_refs = []
    for v in variables:
      ref = {}
      ref['@ItemOID'] = v['uuid']
      ref['@Mandatory'] = v['core']
      ref['@OrderNumber'] = int(v['ordinal'])
      ref['@KeySequence'] = 'tbc'
      variable_refs.append(ref)

    for v in variables:
      ref = {}
      ref['@ItemOID'] = v['uuid']
      ref['@Mandatory'] = v['core']
      ref['@OrderNumber'] = int(v['ordinal'])
      ref['@KeySequence'] = 'tbc'
      variable_refs.append(ref)

    return variable_refs

def item_group_defs(domains):
    igd = []
    for d in domains:
        item = {}
        item['@OID'] = d['uuid']
        item['@Domain'] = d['name']
        item['@Name'] = d['name']
        item['@Repeating'] = 'tbc'
        item['@IsReferenceData'] = 'tbc'
        item['@SASDatasetName'] = d['name']
        item['@def:Structure'] = 'tbc'
        item['@Purpose'] = 'Tabulation'
        item['@def:StandardOID'] = 'STD.1'
        item['@def:ArchiveLocationID'] = '"tbc"'
        item['@def:ArchiveLocationID'] = '"tbc"'
        description = {
          'TranslatedText': {
            '@xml:lang': 'en',
            '#text': d['label']
          }
        }
        item['Description'] = description
        item['def:Class'] = {'@Name': next((x for x,y in DOMAIN_CLASS.items() if d['name'] in y), "Fix")}
        item['ItemRef'] = set_variable_refs(d['variables'])
        item['def:leaf'] = {
                            "@ID": "tbc",
                            "@xlink:href": d['name'].lower()+".xpt",
                            "def:title": d['name'].lower()+".xpt"
                           }
        igd.append(item)
    return igd


def item_defs(domains):
    idfs = []
    for d in domains:
        for v in d['variables']:
          # debug.append(v)
          item = {}
          item['@OID'] = v['uuid']
          item['@Name'] = v['name']
          item['@DataType'] = v['datatype']
          item['@Length'] = 'tbc'
          item['@SASFieldName'] = v['name']
          item['Description'] = {
              "TranslatedText":
              {
                  "@xml:lang": "en",
                  "#text": v['label']
              }
          },
          item['def:Origin'] = {
              "@Type": "tbc",
              "@Source": "Sponsor"
          }
          # debug.append(item)
          idfs.append(item)
    return idfs

def value_list_defs(domains):
    vlds = []
    for d in domains:
        debug.append(f"\ndomain: {d['name']}")
        for v in d['variables']:
          vlms  = [x for x in d['vlm'] if x['uuid'] == v['uuid']]
          if vlms:
            debug.append(f"len(vlm): {len(vlms)}")
            item = {}
            item['@OID'] = f"VL.{v['name']}.{v['uuid']}"
            item_refs = []
            for vlm in vlms:
              ref = {}
              ref['@ItemOID'] = vlm['uuid']
              ref['@OrderNumber'] = 'tbc'
              ref['@Mandatory'] = vlm['uuid']
              ref['def:WhereClauseRef'] = {
                "def:WhereClauseRef":
                      {
                          "@WhereClauseOID": "FIX"
                      }
              }
              item_refs.append(ref)

            item['ItemRef'] = item_refs
            debug.append(item)
            vlds.append(item)
    return vlds

def where_clause_defs(domains):
    wcds = []
    for d in domains:
        debug.append(f"\ndomain: {d['name']}")
        for v in d['variables']:
          vlms  = [x for x in d['vlm'] if x['uuid'] == v['uuid']]
          debug.append(f"len(vlm): {len(vlms)}")
          item = {}
          item['@OID'] = f"VL.{v['name']}.{v['uuid']}"
          item_refs = []
                # "def:WhereClauseDef":
                # [
                #     {
                #         "@OID": "WC.LB.LBTESTCD.SET1.LBSPEC.BLOOD",
                #         "RangeCheck":
                #         [
                #             {
                #                 "@Comparator": "IN",
                #                 "@SoftHard": "Soft",
                #                 "@def:ItemOID": "IT.LB.LBTESTCD",
                #                 "CheckValue":
                #                 [
                #                     "BILI",
                #                     "GLUC"
                #                 ]
                #             },
                #             {
                #                 "@Comparator": "EQ",
                #                 "@SoftHard": "Soft",
                #                 "@def:ItemOID": "IT.LB.LBSPEC",
                #                 "CheckValue": "BLOOD"
                #             }
                #         ]
                #     },
                #     {
                #         "@OID": "WC.LB.LBTESTCD.SET2.LBSPEC.BLOOD",
                #         "RangeCheck":
                #         [
                #             {
                #                 "@Comparator": "IN",
                #                 "@SoftHard": "Soft",
                #                 "@def:ItemOID": "IT.LB.LBTESTCD",
                #                 "CheckValue":
                #                 [
                #                     "BUN",
                #                     "HGB",
                #                     "LYM"
                #                 ]
                #             },
                #             {
                #                 "@Comparator": "EQ",
                #                 "@SoftHard": "Soft",
                #                 "@def:ItemOID": "IT.LB.LBSPEC",
                #                 "CheckValue": "BLOOD"
                #             }
                #         ]
                #     },
          debug.append(item)
          wcds.append(item)
    return wcds

DEFINE_JSON = Path.cwd() / "tmp" / "define.json"
DEFINE_XML = Path.cwd() / "tmp" / "define.xml"

def main():
  define = {}
  # odm_properties(define)
  define['ODM'] = odm_properties()
  define['ODM']['Study'] = study()
  metadata = metadata_version()

  define['ODM']['Study']['MetaDataVersion'] = metadata

  sd_uuid = get_study_design_uuid()
  domains = get_domains_and_variables(sd_uuid)

  # ItemGroupDef
  igd = item_group_defs(domains)
  define['ODM']['Study']['MetaDataVersion']['ItemGroupDef'] = igd

  # ItemGroupDef
  idfs = item_defs(domains)
  define['ODM']['Study']['MetaDataVersion']['ItemDef'] = idfs

  # def:ValueListDef
  vlds = value_list_defs(domains)
  define['ODM']['Study']['MetaDataVersion']['def:ValueListDef'] = vlds

  # def:WhereClauseDef
  wcds = where_clause_defs(domains)
  define['ODM']['Study']['MetaDataVersion']['def:WhereClauseDef'] = wcds
  # CodeList
  # MethodDef
  # def:CommentDef
  # def:leaf


  write_tmp("define-debug.txt",debug)


  # debug.append(define)
  write_tmp_json("define-debug",define)
  json_data = write_define_json(DEFINE_JSON,define)
  # write_define_xml(DEFINE_XML,define)
  # write_define_xml(DEFINE_XML,json_data)
  # write_define_xml1(DEFINE_XML,define)
  # write_define_xml2(DEFINE_XML,json_data)

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
