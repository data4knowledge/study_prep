import json
import pandas as pd
from pathlib import Path
from d4kms_service import Neo4jConnection
import xmltodict
from dicttoxml2 import dicttoxml
from xml.dom.minidom import parseString
from json2xml import json2xml

# from neo_utils import db_is_down

# print("\033[H\033[J") # Clears terminal window in vs code

# debug = []

def write_debug(data):
    TMP_PATH = Path.cwd() / "tmp"
    OUTPUT_FILE = TMP_PATH / 'debug-python.txt'
    print("Writing debug...",OUTPUT_FILE, end="")
    with open(OUTPUT_FILE, 'w') as f:
        for it in data:
            f.write(str(it))
            f.write('\n')
    print(" ...done")

def write_tmp(name, data):
    TMP_PATH = Path.cwd() / "tmp" / "saved_debug"
    OUTPUT_FILE = TMP_PATH / name
    print("Writing file...",OUTPUT_FILE.name,OUTPUT_FILE, end="")
    with open(OUTPUT_FILE, 'w') as f:
        for it in data:
            f.write(str(it))
            f.write('\n')
    print(" ...done")

def write_tmp_json(name, data):
    TMP_PATH = Path.cwd() / "tmp" / "saved_debug"
    OUTPUT_FILE = TMP_PATH / f"{name}.json"
    print("Writing file...",OUTPUT_FILE.name,OUTPUT_FILE, end="")
    json_data = json.dumps(data, indent= 2)
    with open(OUTPUT_FILE, "w") as json_file:
        json_file.write(json_data)
    print(" ...done")

def write_define_json(full_path, data):
    TMP_PATH = Path.cwd() / "tmp" / "saved_debug"
    OUTPUT_FILE = Path(full_path)
    print("Writing file...",OUTPUT_FILE.name,OUTPUT_FILE, end="")
    json_data = json.dumps(data, indent= 2)
    with open(OUTPUT_FILE, "w") as json_file:
        json_file.write(json_data)
    print(" ...done")
    return json_data

def write_define_xml(full_path, data):
    # xml = dicttoxml(data, custom_root='ODM', return_bytes=False, attr_type=False)
    # xml = dicttoxml(data, root=False, return_bytes=False, attr_type=False)
    xml = dicttoxml(data, root=False, attr_type=False)
    # xml = xmltodict.unparse(data,pretty=True)
    dom = parseString(xml)

    OUTPUT_FILE = Path(full_path)
    print("Writing file...",OUTPUT_FILE.name,OUTPUT_FILE, end="")
    json_data = json.dumps(data, indent= 2)
    with open(OUTPUT_FILE, 'w') as f:
        # f.write(xml)
        # f.write('<?xml-stylesheet type="text/xsl" href="../../stylesheets/define2-1.xsl"?>')
        f.write(dom.toprettyxml())
        # f.write('\n')
    print(" ...done")

def write_define_xml2(full_path, data):
    xml = json2xml.Json2xml(data, attr_type=False).to_xml()

    OUTPUT_FILE = Path(full_path)
    print("Writing file...",OUTPUT_FILE.name,OUTPUT_FILE, end="")
    json_data = json.dumps(data, indent= 2)
    with open(OUTPUT_FILE, 'w') as f:
        f.write(xml)
        # f.write('<?xml-stylesheet type="text/xsl" href="../../stylesheets/define2-1.xsl"?>')
        # f.write(dom.toprettyxml())
        # f.write('\n')
    print(" ...done")


def get_xpt_data(file):
    df = pd.read_sas(file,format='xport',encoding='ISO-8859-1')
    df = df.fillna('')
    data = df.to_dict(orient='records')
    return data


def to_debug(*txts):
    global debug
    print("printilintar")
    list = []
    for x in txts:
        list.append(x)
    debug.append(" ".join(list))

def check_lb():
    LB_DATA_FULL = Path.cwd() / "tmp" / "lb.xpt"
    assert LB_DATA_FULL.exists(), "LB_DATA_FULL not found"
    lb_data = get_xpt_data(LB_DATA_FULL)

    # results = lb_data[0].keys()
    # results = [{"testcd":x['LBTESTCD'],"test":x['LBTEST']} for x in lb_data]
    results = [{"testcd":x['LBTESTCD'],"test":x['LBTEST'],"cat":x['LBCAT']} for x in lb_data]
    results = [dict(t) for t in {tuple(d.items()) for d in results}]

    write_tmp("lb_test_cat.txt",results)

def query_db(query):
    db = Neo4jConnection()
    with db.session() as session:
        results = session.run(query)
        if results == None:
            print("Error in query",query)
            exit()
        return [x.data() for x in results]

def get_datapoint_bc_properties():
    # if db_is_down():
    #     return "Neo4j not running"
    query = f"""
    MATCH (dp:DataPoint)
    return count(dp) as count
    """
    results = query_db(query)
    if results[0]['count'] == 0:
        return print("No DataPoints labels in db - count:",results[0]['count'])
    print("DataPoints exist in db")
    # WHERE dp.uri in ['01-701-1097/LB/135_0/ALT/result','01-701-1015/VS/22_0/DIABP/result']
    query = f"""
        match (dp:DataPoint)
        MATCH (dp)-[:FOR_DC_REL]->(dc:DataContract)
        MATCH (dc)-[:PROPERTIES_REL]->(bcp:BiomedicalConceptProperty)
        MATCH (bcp)<-[:PROPERTIES_REL]-(bc:BiomedicalConcept)
        MATCH (dc)-[:INSTANCES_REL]->(main_sai:ScheduledActivityInstance)
        MATCH (main_sai)-[:ENCOUNTER_REL]->(enc:Encounter)
        optional MATCH (dc)-[:INSTANCES_REL]->(sub_sai:ScheduledActivityInstance)
        optional MATCH (sub_sai)<-[:RELATIVE_FROM_SCHEDULED_INSTANCE_REL]-(t:Timing)
        where main_sai.id <> sub_sai.id
        return distinct bc.name as bc, bcp.name as property, enc.name as encounter, t.name as tpt
    """
    results = query_db(query)
    if results == None:
        print("Error in query")
    else:
        if results == []:
            print("Query returned no results")
        else:
            write_tmp('db_datapoints.txt',results)

def check_data_contract_exist():
    query = f"""
        MATCH (dc:DataContract)
        MATCH (dc)-[:PROPERTIES_REL]->(bcp:BiomedicalConceptProperty)
        MATCH (bcp)<-[:PROPERTIES_REL]-(bc:BiomedicalConcept)
        MATCH (dc)-[:INSTANCES_REL]->(main_sai:ScheduledActivityInstance)
        MATCH (main_sai)-[:ENCOUNTER_REL]->(enc:Encounter)
        MATCH (dc)-[:INSTANCES_REL]->(sub_sai:ScheduledActivityInstance)
        MATCH (sub_sai)<-[:RELATIVE_FROM_SCHEDULED_INSTANCE_REL]-(t:Timing)
        where main_sai.id <> sub_sai.id
        return bc.name as bc, bcp.name as property, enc.name as encounter, t.name as tpt
        limit 10
    """
    # results = query_db(query)
    # if results[0]['count'] == 0:
    #     return print("No DataPoints labels in db - count:",results[0]['count'])
    # print("DataPoints exist in db")
    # # WHERE dp.uri in ['01-701-1097/LB/135_0/ALT/result','01-701-1015/VS/22_0/DIABP/result']
    # query = f"""
    #     match (dp:DataPoint)
    #     MATCH (dp)-[:FOR_DC_REL]->(dc:DataContract)
    #     MATCH (dc)-[:PROPERTIES_REL]->(bcp:BiomedicalConceptProperty)
    #     MATCH (bcp)<-[:PROPERTIES_REL]-(bc:BiomedicalConcept)
    #     MATCH (dc)-[:INSTANCES_REL]->(main_sai:ScheduledActivityInstance)
    #     MATCH (main_sai)-[:ENCOUNTER_REL]->(enc:Encounter)
    #     optional MATCH (dc)-[:INSTANCES_REL]->(sub_sai:ScheduledActivityInstance)
    #     optional MATCH (sub_sai)<-[:RELATIVE_FROM_SCHEDULED_INSTANCE_REL]-(t:Timing)
    #     where main_sai.id <> sub_sai.id
    #     return distinct bc.name as bc, bcp.name as property, enc.name as encounter, t.name as tpt
    # """
    # results = query_db(query)
    # if results == None:
    #     print("Error in query")
    # else:
    #     if results == []:
    #         print("Query returned no results")
    #     else:
    #         write_tmp('db_datapoints.txt',results)

if __name__ == "__main__":
    # check_lb()
    get_datapoint_bc_properties()
