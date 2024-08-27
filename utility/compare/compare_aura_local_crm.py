import json
# import pandas as pd
from pathlib import Path
from neo4j import GraphDatabase
from d4kms_service import Neo4jConnection, ServiceEnvironment
from utility.neo_utils import db_is_down
from utility.mappings import DATA_LABELS_TO_BC_LABELS, DATA_VISITS_TO_ENCOUNTER_LABELS, DATA_TPT_TO_TIMING_LABELS
from utility.debug import write_debug, write_tmp

debug = []

def query_aura(query):
    # CRM
    AURA_CONNECTION_URI="neo4j+s://2f566ce1.databases.neo4j.io"
    AURA_USERNAME="neo4j"
    AURA_PASSWORD="d4qfoPx42uQOKTTvOWUf54_S12pqyaJP4lRq-tmijlQ"

    # Driver instantiation
    driver = GraphDatabase.driver(AURA_CONNECTION_URI,auth=(AURA_USERNAME, AURA_PASSWORD))

    data = []

    with driver.session() as session:
        # Use .data() to access the results array
        results = session.run(query).data()
        # data = [r.data() for r in results]

    driver.close()
    
    return results


def query_local(query):
    NEO4J_CONNECTION_URI="bolt://localhost:7687"
    NEO4J_USERNAME="neo4j"
    NEO4J_PASSWORD="adminadmin"
    NEO4J_DB="crm-service-dev"

    # Driver instantiation
    driver = GraphDatabase.driver(NEO4J_CONNECTION_URI,auth=(NEO4J_USERNAME, NEO4J_PASSWORD))

    # session = self._driver.session(database=self._db_name)
    # response = list(session.run(query))

    data = []

    with driver.session(database=NEO4J_DB) as session:
        results = session.run(query).data()
        # response = session.run(query)
        # results = [result.data() for result in response]
        # data = [r.data() for r in results]
    driver.close()
    
    # return [result.data() for result in results]
    return results

def add_to_debug(results):
    if results:
        for result in results:
            debug.append(result)
    else:
        debug.append("No results from local")
        print("No results from local")

def query_both(query):
    local_results = query_local(query)
    aura_results = query_aura(query)
    return {"local":local_results,"aura":aura_results}

def compare_crm():
    print("Connecting to local Neo4j...",end="")
    if db_is_down():
        print("is not running")
        exit()
    print("connected")

    # query = """
    # call db.labels()
    # """
    # debug.append("-- local")
    # add_to_debug([x['label'] for x in results['local']])
    # debug.append("-- aura")
    # add_to_debug([x['label'] for x in results['aura']])
    # results = query_both(query)

    query = """
    match (m:ModelNode)
    return m
    order by m.uri
    """
    results = query_both(query)
    l = []
    # for result in results['local']:
    #     l.append(result)
    for result in results['aura']:
        debug.append(result)
    # add_to_debug(results['local'])
    # debug.append("-- aura")
    # add_to_debug(results['aura'])


    write_tmp("result-crm.txt",debug)


if __name__ == "__main__":
    compare_crm()

# All ModelNodes
# {'uri': 'https://crm.d4k.dk/dataset/adverse_event'}
# {'uri': 'https://crm.d4k.dk/dataset/adverse_event/category'}
# {'uri': 'https://crm.d4k.dk/dataset/adverse_event/causality'}
# {'uri': 'https://crm.d4k.dk/dataset/adverse_event/causality/device'}
# {'uri': 'https://crm.d4k.dk/dataset/adverse_event/causality/non_study_treatment'}
# {'uri': 'https://crm.d4k.dk/dataset/adverse_event/causality/related'}
# {'uri': 'https://crm.d4k.dk/dataset/adverse_event/response'}
# {'uri': 'https://crm.d4k.dk/dataset/adverse_event/response/concomitant_treatment'}
# {'uri': 'https://crm.d4k.dk/dataset/adverse_event/response/other'}
# {'uri': 'https://crm.d4k.dk/dataset/adverse_event/response/study_treatment'}
# {'uri': 'https://crm.d4k.dk/dataset/adverse_event/serious'}
# {'uri': 'https://crm.d4k.dk/dataset/adverse_event/severity'}
# {'uri': 'https://crm.d4k.dk/dataset/adverse_event/term'}
# {'uri': 'https://crm.d4k.dk/dataset/adverse_event/toxicity'}
# {'uri': 'https://crm.d4k.dk/dataset/adverse_event/toxicity/grade'}
# {'uri': 'https://crm.d4k.dk/dataset/common/date_time'}
# {'uri': 'https://crm.d4k.dk/dataset/common/location'}
# {'uri': 'https://crm.d4k.dk/dataset/common/location/directionality'}
# {'uri': 'https://crm.d4k.dk/dataset/common/location/laterality'}
# {'uri': 'https://crm.d4k.dk/dataset/common/location/portion'}
# {'uri': 'https://crm.d4k.dk/dataset/common/period'}
# {'uri': 'https://crm.d4k.dk/dataset/common/period/period_end'}
# {'uri': 'https://crm.d4k.dk/dataset/common/period/period_start'}
# {'uri': 'https://crm.d4k.dk/dataset/observation'}
# {'uri': 'https://crm.d4k.dk/dataset/observation/body_system'}
# {'uri': 'https://crm.d4k.dk/dataset/observation/conscious'}
# {'uri': 'https://crm.d4k.dk/dataset/observation/epidemic_/_pandemic'}
# {'uri': 'https://crm.d4k.dk/dataset/observation/fasting'}
# {'uri': 'https://crm.d4k.dk/dataset/observation/lab'}
# {'uri': 'https://crm.d4k.dk/dataset/observation/lead'}
# {'uri': 'https://crm.d4k.dk/dataset/observation/loinc_ref'}
# {'uri': 'https://crm.d4k.dk/dataset/observation/method'}
# {'uri': 'https://crm.d4k.dk/dataset/observation/observation_result'}
# {'uri': 'https://crm.d4k.dk/dataset/observation/observation_result/chronicity'}
# {'uri': 'https://crm.d4k.dk/dataset/observation/observation_result/coded'}
# {'uri': 'https://crm.d4k.dk/dataset/observation/observation_result/distribution'}
# {'uri': 'https://crm.d4k.dk/dataset/observation/observation_result/result'}
# {'uri': 'https://crm.d4k.dk/dataset/observation/observation_result/result_other'}
# {'uri': 'https://crm.d4k.dk/dataset/observation/observation_result/scale'}
# {'uri': 'https://crm.d4k.dk/dataset/observation/observation_result/type'}
# {'uri': 'https://crm.d4k.dk/dataset/observation/position'}
# {'uri': 'https://crm.d4k.dk/dataset/observation/range'}
# {'uri': 'https://crm.d4k.dk/dataset/observation/range/hi'}
# {'uri': 'https://crm.d4k.dk/dataset/observation/range/low'}
# {'uri': 'https://crm.d4k.dk/dataset/observation/reason_performed'}
# {'uri': 'https://crm.d4k.dk/dataset/observation/reference_result'}
# {'uri': 'https://crm.d4k.dk/dataset/observation/run_id'}
# {'uri': 'https://crm.d4k.dk/dataset/observation/sensitivity'}
# {'uri': 'https://crm.d4k.dk/dataset/observation/specimen'}
# {'uri': 'https://crm.d4k.dk/dataset/observation/specimen/anatomic_region'}
# {'uri': 'https://crm.d4k.dk/dataset/observation/specimen/condition'}
# {'uri': 'https://crm.d4k.dk/dataset/observation/specimen/specimen_type'}
# {'uri': 'https://crm.d4k.dk/dataset/observation/specimen/usable'}
# {'uri': 'https://crm.d4k.dk/dataset/observation/test'}
# {'uri': 'https://crm.d4k.dk/dataset/therapeutic_intervention'}
# {'uri': 'https://crm.d4k.dk/dataset/therapeutic_intervention/adjustment'}
# {'uri': 'https://crm.d4k.dk/dataset/therapeutic_intervention/daily_dose'}
# {'uri': 'https://crm.d4k.dk/dataset/therapeutic_intervention/description'}
# {'uri': 'https://crm.d4k.dk/dataset/therapeutic_intervention/form'}
# {'uri': 'https://crm.d4k.dk/dataset/therapeutic_intervention/frequency'}
# {'uri': 'https://crm.d4k.dk/dataset/therapeutic_intervention/route'}
# {'uri': 'https://crm.d4k.dk/dataset/therapeutic_intervention/single_dose'}
# {'uri': 'https://crm.d4k.dk/dataset/therapeutic_intervention/treatment'}
# {'uri': 'https://crm.d4k.dk/dataset/therapeutic_intervention/treatment_vehicle'}
# {'uri': 'https://crm.d4k.dk/dataset/therapeutic_intervention/treatment_vehicle/amount'}
# {'uri': 'https://crm.d4k.dk/dataset/therapeutic_intervention/treatment_vehicle/carrier'}
