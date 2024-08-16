from pathlib import Path
from d4kms_service import Neo4jConnection

debug = []

def write_debug(data):
    TMP_PATH = Path.cwd() / "tmp" / "saved_debug"
    OUTPUT_FILE = TMP_PATH / "ae-match-debug.txt"
    print("Writing file...",OUTPUT_FILE.name,OUTPUT_FILE, end="")
    with open(OUTPUT_FILE, 'w') as f:
        for it in data:
            f.write(str(it))
            f.write('\n')
    print(" ...done")


def set_ae_datapoints_unassigned():
  db = Neo4jConnection()
  with db.session() as session:
    query = """
      MATCH (bc:BiomedicalConcept)-[:PROPERTIES_REL]->(bcp:BiomedicalConceptProperty)
      MATCH (bcp)<-[:PROPERTIES_REL]->(dc:DataContract)
      MATCH (dc)<-[:FOR_DC_REL]->(dp:DataPoint)
      WHERE bc.name = "Adverse Event Prespecified"
      WITH dp
      SET dp.status = "unassigned"
      return count(dp) as count
    """
    results = session.run(query)
    print("Set status unassigned",next(results))
  db.close()
  return

def get_visit_dates():
  db = Neo4jConnection()
  with db.session() as session:
    query = """
      MATCH (:BiomedicalConcept {name:"Adverse Event Prespecified"})-[:PROPERTIES_REL]->(aebcp:BiomedicalConceptProperty)<-[:PROPERTIES_REL]->(aedc:DataContract)
      MATCH (aedc)<-[:FOR_DC_REL]->(aedp:DataPoint)
      with aedc, aedp
      MATCH (aedp)-[:FOR_SUBJECT_REL]->(subj:Subject)
      MATCH (subj)<-[:FOR_SUBJECT_REL]-(dtcdp:DataPoint)-[:FOR_DC_REL]->(dc:DataContract)
      MATCH (dc)-[:PROPERTIES_REL]->(bcp:BiomedicalConceptProperty)-[:IS_A_REL]-(crm:CRMNode)
      MATCH (dc)-[:INSTANCES_REL]->(sai:ScheduledActivityInstance)
      MATCH (sai)-[:ENCOUNTER_REL]->(enc:Encounter)
      // MATCH (sai)-[:EPOCH_REL]->(epoch:StudyEpoch)
      where crm.datatype = "date_time"
      with distinct 
      subj.identifier as usubjid
      // ,enc.name as e_name
      ,enc.label as encounter
      ,min(dtcdp.value) as start_date
      ,max(dtcdp.value) as end_date
      order by usubjid, start_date
      return distinct 
      usubjid
      ,encounter
      ,start_date
      ,end_date
    """
    results = session.run(query)
    res = [result.data() for result in results]
  return res

def match_ae_datapoints(visits):
  db = Neo4jConnection()
  # Get AE "records" start date
  with db.session() as session:
      query = """
        MATCH (bc:BiomedicalConcept {name: "Adverse Event Prespecified"})-[:PROPERTIES_REL]->(bcp:BiomedicalConceptProperty)
        MATCH (bcp)<-[:PROPERTIES_REL]->(dc:DataContract)<-[:FOR_DC_REL]->(dp:DataPoint)-[:SOURCE]->(r:Record)
        MATCH (dp)-[:FOR_SUBJECT_REL]->(subj:Subject)
        MATCH (dc)-[:PROPERTIES_REL]->(bcp:BiomedicalConceptProperty)-[:IS_A_REL]-(crm:CRMNode)
        WHERE crm.sdtm = "--STDTC"
        with subj, r, dp
        return
        subj.identifier as usubjid
        ,r.key as record_key
        ,dp.value as start_date
      """
      # print("query",query)
      results = session.run(query)
      aes = [result.data() for result in results]
  db.close()

  # for ae in aes:
  #   debug.append(ae)

  subjects = list(set([item['usubjid'] for item in aes]))
  # print("subjects",subjects)

  for subject in subjects:
    debug.append(f"doing subject {subject}")
    print("doing subject",subject)
    subj_dates = [item['start_date'] for item in visits if item['usubjid'] == subject]
    debug.append(subj_dates)
    # for x in subj_dates:
    #   debug.append(x)
    subj_ae = [item for item in aes if item['usubjid'] == subject]
    for ae in subj_ae:
      debug.append("")
      debug.append(ae)
      match = next((date for date in subj_dates if ae['start_date'] <= date),None)
      if match:
        idx = subj_dates.index(match)
        debug.append(f"idx: {idx} {match}")
        debug.append(f"match:{match} < {ae['start_date']}")
        visit = next((item for item in visits if item['usubjid'] == subject and item['start_date'] == match),None)
        debug.append(visit)
      else:
        debug.append("no match")

  # Match AE records with visits

  # with db.session() as session:
  #     for item in items:
  #         query = f"""
  #             MATCH (dc:DataContract {{uri:'{item['data_contract']}'}})
  #             WITH COUNT(dc) > 0  as dc_exists
  #             RETURN dc_exists as exist
  #         """
  #         # print(query)
  #         results = session.run(query)
  #         if results[0].data()['exist']:
  #             pass
  #         else:
  #             print("\n---\ndata_contract MISSING :",item['data_contract'])

if __name__ == "__main__":
  # clear_created_nodes()
  set_ae_datapoints_unassigned()
  visits = get_visit_dates()
  # for x in visits:
  #    debug.append(x)
 
  match_ae_datapoints(visits)

  write_debug(debug)
