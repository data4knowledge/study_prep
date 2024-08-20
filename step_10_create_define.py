import json
from pathlib import Path
from d4kms_service import Neo4jConnection
from model.configuration import Configuration, ConfigurationNode
from model.base_node import BaseNode
from utility.debug import write_debug, write_tmp, write_tmp_json, write_define_json, write_define_xml, write_define_xml2
import xmlschema

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
       ,'VSLOC'  :'https://crm.d4k.dk/dataset/observation/location/coding/code'
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
        print("query",query)



def to_debug(*txts):
    global debug
    print("printilintar")
    list = []
    for x in txts:
        list.append(x)
    debug.append(" ".join(list))

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
      print("variables query", query)
      results = session.run(query)
      vars_in_use = [r['v'] for r in results]
      return vars_in_use


def get_define_first(domain_uuid):
    db = Neo4jConnection()
    with db.session() as session:
      # query = """
      # MATCH (sd:StudyDesign)-[:DOMAIN_REL]->(domain:Domain {uuid:'%s'})
      # MATCH (sd)<-[:STUDY_DESIGNS_REL]-(sv:StudyVersion)
      # MATCH (sv)-[:STUDY_IDENTIFIERS_REL]->(si:StudyIdentifier)-[:STUDY_IDENTIFIER_SCOPE_REL]->(sis:Organization {name:'Eli Lilly'})
      # WITH si, domain
      # MATCH (domain)-[:USING_BC_REL]-(bc:BiomedicalConcept)-[:PROPERTIES_REL]->(bcp:BiomedicalConceptProperty)
      # MATCH (bcp)<-[:PROPERTIES_REL]->(dc:DataContract)
      # MATCH (dc)<-[:FOR_DC_REL]-(:DataPoint)-[:SOURCE]->(r:Record)
      # WITH si, domain, dc, r
      # MATCH (r)<-[:SOURCE]-(dp:DataPoint)-[:FOR_SUBJECT_REL]->(subj:Subject)
      # MATCH (dp)-[:FOR_DC_REL]->(dc:DataContract)
      # MATCH (dc)-[:PROPERTIES_REL]->(bcp:BiomedicalConceptProperty)<-[:PROPERTIES_REL]-(bc:BiomedicalConcept)
      # return
      # si.studyIdentifier as STUDYID
      # , domain.name as DOMAIN
      # , r.key as key
      # , CASE bcp.name WHEN '--DTC' THEN 'AEDTC' ELSE bcp.name END as variable
      # , dp.value as value
      # // , site.name as SITEID
      # // , e.label as VISIT
      # // , epoch.label as EPOCH
      # , bc.uuid as bc_uuid
      # order by key
      # limit 100
      # """ % (domain_uuid)

      #       query = """
      #       MATCH (sd:StudyDesign {uuid:'%s'})-[:DOMAIN_REL]->(domain:Domain)
      #       MATCH (sd)<-[:STUDY_DESIGNS_REL]-(sv:StudyVersion)
      #       MATCH (sv)-[:STUDY_IDENTIFIERS_REL]->(si:StudyIdentifier)-[:STUDY_IDENTIFIER_SCOPE_REL]->(sis:Organization {name:'Eli Lilly'})
      #       WITH si, domain
      #       MATCH (domain)-[:USING_BC_REL]-(bc:BiomedicalConcept)-[:PROPERTIES_REL]->(bcp:BiomedicalConceptProperty)
      #       MATCH (bcp)<-[:PROPERTIES_REL]->(dc:DataContract)
      #       MATCH (dc)<-[:FOR_DC_REL]-(:DataPoint)-[:SOURCE]->(r:Record)
      #       WITH si, domain, dc, r
      #       MATCH (r)<-[:SOURCE]-(dp:DataPoint)-[:FOR_SUBJECT_REL]->(subj:Subject)
      #       MATCH (dp)-[:FOR_DC_REL]->(dc:DataContract)
      #       MATCH (dc)-[:PROPERTIES_REL]->(bcp:BiomedicalConceptProperty)<-[:PROPERTIES_REL]-(bc:BiomedicalConcept)
      #       return
      #       si.studyIdentifier as STUDYID
      #       , domain.name as DOMAIN
      # //      , subj.identifier as USUBJID
      # //      , right(subj.identifier,6) as SUBJECT
      #       , r.key as key
      #       , CASE bcp.name WHEN '--DTC' THEN 'AEDTC' ELSE bcp.name END as variable
      #       , dp.value as value
      #       // , site.name as SITEID
      #       // , e.label as VISIT
      #       // , epoch.label as EPOCH
      #       , bc.uuid as bc_uuid
      #       order by key
      #       """ % (sd_uuid)

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
        WITH bc, bcp, crm, var, collect(c.decode) as decode
        return distinct 
        bc.name as bc,
        bcp.name as bcp,
        crm.sdtm as aa,
        var.name as var,
        decode as decode
      """ % (domain_uuid)
      # limit 100
      print("define query", query)
      results = session.run(query)
      return [r.data() for r in results]
   


def get_domains_and_variables(uuid):
   domains = get_domains(uuid)
   for d in domains:
      print("domain",d['name'])
      variables = get_variables(d['uuid'])
      print("len(variables)", len(variables))
      define_metadata = get_define_first(d['uuid'])
      print("len(define_metadata)", len(define_metadata))
      for x in define_metadata:
        debug.append(x)


   return domains

def item_group_defs(domains):
    igd = []
    for d in domains:
        item = {}
        item['@OID'] = d['uuid']
        item['@Domain'] = d['name']
        item['@Name'] = d['name']
        item['@Repeating'] = 'tbc'
        item['@IsReferenceData'] = 'tbc'
        item['@def:Structure'] = 'tbc'
        item['@Purpose'] = 'Tabulation'
        item['@def:StandardOID'] = 'STD.1'
        igd.append(item)
        # print(item)
        # "@Name": "DM",
        # "@Repeating": "No",
        # "@IsReferenceData": "No",
        # "@SASDatasetName": "DM",
        # "@def:Structure": "One record per subject",
        # "@Purpose": "Tabulation",
        # "@def:StandardOID": "STD.1",
        # "@def:CommentOID": "COM.DOMAIN.DM",
        # "@def:ArchiveLocationID": "LF.DM",
        # "Description": {
        #   "TranslatedText": {
        #     "@xml:lang": "en",
        #     "#text": "Demographics"
        #   }
    return igd

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
  print("sd_uuid", sd_uuid)

  domains = get_domains_and_variables(sd_uuid)

  igd = item_group_defs(domains)
  define['ODM']['Study']['MetaDataVersion']['ItemGroupDef'] = igd

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
    _add_missing_links_to_crm()
    main()
    # check_define()
