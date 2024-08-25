from d4kms_service import Neo4jConnection

query_debug = []

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
        query_debug.append([v for k,v in x.items()])
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

def get_study_info():
    db = Neo4jConnection()
    with db.session() as session:
      # query = """
      #   MATCH (sd:StudyDesign)<-[:STUDY_DESIGNS_REL]-(sv:StudyVersion)
      #   MATCH (sv)-[:STUDY_IDENTIFIERS_REL]->(si:StudyIdentifier)-[:STUDY_IDENTIFIER_SCOPE_REL]->(:Organization {name:'Eli Lilly'})
      #   MATCH (sv)-[:DOCUMENT_VERSION_REL]->(spdv:StudyProtocolDocumentVersion)<-[:VERSIONS_REL]->(spd:StudyProtocolDocument)
      #   return *
      #   """
      # results = session.run(query)
      # data = [r.data() for r in results]
      # for x in data:
      #   #  query_debug.append(x)
      #    for k,v in x.items():
      #     query_debug.append(f"{k}")
      #     for k1,v1 in v.items():
      #       query_debug.append(f"  {k1}: {v1}")
            
      query = """
        MATCH (sd:StudyDesign)<-[:STUDY_DESIGNS_REL]-(sv:StudyVersion)
        MATCH (sv)-[:STUDY_IDENTIFIERS_REL]->(si:StudyIdentifier)-[:STUDY_IDENTIFIER_SCOPE_REL]->(:Organization {name:'Eli Lilly'})
        MATCH (sv)-[:DOCUMENT_VERSION_REL]->(spdv:StudyProtocolDocumentVersion)<-[:VERSIONS_REL]->(spd:StudyProtocolDocument)
        return 
        sd.uuid as uuid,
        si.studyIdentifier as study_name,
        sd.description as description,
        sv.rationale as rationale,
        spd.name as protocol_name
        """
      # query_debug.append(query)
      results = session.run(query)
      data = [r.data() for r in results]
    db.close()
    return data[0]

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
        ORDER BY v.ordinal
      """ % (uuid)
      # print("variables query", query)
      results = session.run(query)
      # all_variables = [r['v'] for r in results]
      all_variables = [r['v'] for r in results.data()]
      required_variables = [v for v in all_variables if v['core'] == 'Req']

      # CRM linked vars
      query = """
        MATCH (d:Domain {uuid: '%s'})-[]->(v:Variable)-[:IS_A_REL]->(:CRMNode) RETURN v
        ORDER BY v.name
      """ % (uuid)
      # print("variables query", query)
      results = session.run(query)
      vars_in_use = [r['v'] for r in results.data()]
    db.close()
    # return vars_in_use
    return all_variables


def get_define_vlm(domain_uuid):
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
        bc.uuid as bc_uuid,
        bc.name as bc,
        cd.decode as testcd,
        bcp.name as bcp,
        crm.datatype as datatype,
        var.uuid as uuid,
        var.label as label,
        var.name as name,
        var.core as core,
        var.ordinal as ordinal,
        decodes as decodes
//        order by bc_uuid, ordinal
      """ % (domain_uuid)
      # limit 100
      # print("vlm query", query)
      results = session.run(query)
      data = [r for r in results.data()]
      # query_debug.append("vlm query")
      # query_debug.append(query)
      # query_debug.append("vlm--->")
      # for d in data:
      #    query_debug.append(d)
      # query_debug.append("vlm<---")
    db.close()
    return data

def get_define_codelist(domain_uuid):
    db = Neo4jConnection()
    with db.session() as session:
      query = """
        MATCH (sd:StudyDesign)-[:DOMAIN_REL]->(domain:Domain {uuid:'%s'})
        MATCH (sd)<-[:STUDY_DESIGNS_REL]-(sv:StudyVersion)
        MATCH (sv)-[:STUDY_IDENTIFIERS_REL]->(si:StudyIdentifier)-[:STUDY_IDENTIFIER_SCOPE_REL]->(:Organization {name:'Eli Lilly'})
        WITH si, domain
        MATCH (domain)-[:USING_BC_REL]-(bc:BiomedicalConcept)-[:PROPERTIES_REL]->(bcp:BiomedicalConceptProperty)
        MATCH (bc)-[:CODE_REL]-(:AliasCode)-[:STANDARD_CODE_REL]->(bc_cd:Code)
        WITH bc, bc_cd, bcp, domain
        MATCH (bcp)-[:IS_A_REL]->(crm:CRMNode)<-[:IS_A_REL]-(var:Variable)<-[:VARIABLE_REL]-(domain)
        MATCH (bcp)-[:RESPONSE_CODES_REL]->(rc:ResponseCode)-[:CODE_REL]->(c:Code)
        // WHERE  var.label = bcp.label
        // or bcp.name = crm.sdtm
        WITH bc, bc_cd, bcp, crm, var, c
        ORDER By bc.name, bc_cd.decode, bcp.name, c.decode
        WITH bc, bc_cd, bcp, crm, var, collect({code:c.code,decode:c.decode}) as decodes
        return distinct 
        // bc.uuid as bc_uuid,
        bc.name as bc,
        bc_cd.decode as testcd,
        var.uuid as uuid,
        var.label as label,
        var.name as name,
        crm.datatype as datatype,
        var.ordinal as ordinal,
        decodes as decodes
        order by name
      """ % (domain_uuid)
      query_debug.append("codelist query")
      query_debug.append(query)
      results = session.run(query)
      # return [r.data() for r in results]
      data = [r for r in results.data()]
      query_debug.append("codelist--->")
      for d in data:
         query_debug.append(d)
      query_debug.append("codelist<---")
    db.close()
    return data
