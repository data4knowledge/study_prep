import os
import json
import csv
from pathlib import Path
from d4kms_service import Neo4jConnection
from utility.mappings import DATA_LABELS_TO_BC_LABELS, DATA_VISITS_TO_ENCOUNTER_LABELS, DATA_TPT_TO_TIMING_LABELS, TEST_ROW_VARIABLE_TO_BC_PROPERTY_NAME

def write_tmp(name, data):
    TMP_PATH = Path.cwd() / "tmp" / "saved_debug"
    OUTPUT_FILE = TMP_PATH / name
    print("Writing file...",OUTPUT_FILE.name,OUTPUT_FILE, end="")
    with open(OUTPUT_FILE, 'w') as f:
        for it in data:
            f.write(str(it))
            f.write('\n')
    print(" ...done")

debug = []
matches = []
issues = []
collection_datapoints = {}

DATA_CONTRACTS_LOOKUP = Path.cwd() / "data" / "output" / "data_contracts.json"
assert DATA_CONTRACTS_LOOKUP.exists(), "DATA_CONTRACTS_LOOKUP not found"
print("\nGetting data contracts from file",DATA_CONTRACTS_LOOKUP)
with open(DATA_CONTRACTS_LOOKUP) as f:
    data_contracts = json.load(f)

def clear_created_nodes():
    db = Neo4jConnection()
    with db.session() as session:
        query = "match (n:Datapoint|DataPoint) detach delete n return count(n)"
        results = session.run(query)
        print("Removing Datapoint/DataPoint",results)
        query = "match (n:Subject) detach delete n return count(n)"
        results = session.run(query)
        print("Removing Subject",results)
    db.close()

def get_bc_properties( bc_label, row):
    results = []
    if row['VISIT'] in DATA_VISITS_TO_ENCOUNTER_LABELS:
        visit = DATA_VISITS_TO_ENCOUNTER_LABELS[row['VISIT']]
    else:
        add_issue("visit not found:",row['VISIT'])
        return []
    query = f"""
        MATCH (bc:BiomedicalConcept)-[:PROPERTIES_REL]->(bcp)<-[:PROPERTIES_REL]-(dc:DataContract)-[:INSTANCES_REL]->(act_inst)-[:ENCOUNTER_REL]-(enc)
        WHERE enc.label = '{visit}'
        AND  bc.label = '{bc_label}'
        return bc.label as BC_LABEL, bcp.name as BCP_NAME, bcp.label as BCP_LABEL, enc.label as ENCOUNTER_LABEL, dc.uri as DC_URI
    """
    results = db_query(query)
    if results == []:
        add_issue("DataContract has errors in it",row['VISIT'],visit,bc_label,query)
        return []
    return results

def get_bc_properties_sub_timeline(bc_label, tpt, row):
    if row['VISIT'] in DATA_VISITS_TO_ENCOUNTER_LABELS:
        visit = DATA_VISITS_TO_ENCOUNTER_LABELS[row['VISIT']]
    else:
        add_issue("visit not found:",row['VISIT'])
        return []
    query = f"""
        match (msai:ScheduledActivityInstance)<-[:INSTANCES_REL]-(dc:DataContract)-[:INSTANCES_REL]-(ssai:ScheduledActivityInstance)
        match (msai)-[:ENCOUNTER_REL]->(enc:Encounter)
        match (ssai)<-[:RELATIVE_FROM_SCHEDULED_INSTANCE_REL]-(t:Timing)
        match (dc)-[:PROPERTIES_REL]->(bcp:BiomedicalConceptProperty)<-[:PROPERTIES_REL]-(bc:BiomedicalConcept)
        WHERE enc.label = '{visit}'
        and    t.value = '{tpt}'
        AND  bc.label = '{bc_label}'
        return bc.label as BC_LABEL, bcp.name as BCP_NAME, bcp.label as BCP_LABEL, enc.label as ENCOUNTER_LABEL, t.value as TIMEPOINT_VALUE, dc.uri as DC_URI
    """
    results = db_query(query)
    if results == []:
        add_issue("timeline DataContract query has errors in it",visit,bc_label,tpt,query)
        return []
    else:
        return results

def get_bc_properties_ae(bc_label):
    query = f"""
        match (msai:ScheduledActivityInstance)<-[:INSTANCES_REL]-(dc:DataContract)-[:INSTANCES_REL]-(ssai:ScheduledActivityInstance)
        match (ssai)<-[:RELATIVE_FROM_SCHEDULED_INSTANCE_REL]-(t:Timing)
        match (dc)-[:PROPERTIES_REL]->(bcp:BiomedicalConceptProperty)<-[:PROPERTIES_REL]-(bc:BiomedicalConcept)
        WHERE bc.label = '{bc_label}'
        return bc.label as BC_LABEL, bcp.name as BCP_NAME, bcp.label as BCP_LABEL, dc.uri as DC_URI,"" as ENCOUNTER_LABEL
    """
    # print("ae query", query)
    results = db_query(query)
    if results == []:
        add_issue("DataContract has errors in it",bc_label,query)
        return []
    return results



def add_issue(*txts):
    add = []
    for txt in txts:
        if txt == None:
            add.append("None")
        else:
            add.append(txt)
    issues.append(" ".join(add))

def add_row_dp(domain: str, variables: list, row, dp_uri = None):
    keys = []
    keys.append(domain)
    for var in variables:
        if var in row.keys():
            keys.append(str(row[var]))

    key = "_".join(keys)
    if dp_uri == None:
        collection_datapoints[str(key)] = []
    else:
        collection_datapoints[str(key)].append(dp_uri)

def db_query(query):
    db = Neo4jConnection()
    result = []
    with db.session() as session:
        response = session.run(query)
        if response == None:
            print('query did not work"',query)
            exit()
        result = [x.data() for x in response]
    db.close()
    return result

def check_dc_in_file(path: Path, name):
    # Check that DC exist and make separate output files for each 
    OUTPUT_FILE = path / f"{name}.json"
    with open(OUTPUT_FILE) as f:
        data = json.load(f)
    dc_uris = list(set([x['DC_URI'] for x in data]))
    TMP_PATH = Path.cwd() / "tmp" / "bc_data_files"
    assert TMP_PATH.exists(), f"TMP_PATH not found: {TMP_PATH}"
    output_dc_uri_files = {}
    for dc_uri in dc_uris:
        query = f"""
            match (dc:DataContract)-[:PROPERTIES_REL]->(bcp:BiomedicalConceptProperty)
            WHERE dc.uri = '{dc_uri}'
            return bcp.name as name
        """
        bcp_names = db_query(query)
        if bcp_names:
            bcp_name = bcp_names[0]['name']
            output_dc_uri_files[bcp_name] = dc_uri
        else:
            print("\n!-!-!-!-!-!-!-!")
            print("!!! Data Conctract not found:",dc_uri)

    # for bcp_name, dc_uri in output_dc_uri_files.items():
    #     # print("matching bcp - uri",bcp_name, dc_uri)
    #     bcp_data = [item for item in data if item['DC_URI'] == dc_uri]
    #     output_csv(TMP_PATH,f"{name}-{bcp_name.replace(' ','')}.csv",bcp_data)

def clean(txt):
    result = ""
    if isinstance(txt, str):
        result = txt
    elif isinstance(txt, float):
        result = str(txt)
    else:
        print("clean does not now of:",txt.__class__,txt)

    return result.replace(".","_").replace(" ","/")


def get_property_for_variable(test,variable):
    property = None
    if test in TEST_ROW_VARIABLE_TO_BC_PROPERTY_NAME and variable in TEST_ROW_VARIABLE_TO_BC_PROPERTY_NAME[test]:
        property = TEST_ROW_VARIABLE_TO_BC_PROPERTY_NAME[test][variable]
    else:
        print(f"Add property for test:{test} - property/variable:{variable}")
    return property

def get_encounter(row):
    encounter = "NOT SET"
    if 'VISIT' in row:
        if row['VISIT'] in DATA_VISITS_TO_ENCOUNTER_LABELS:
            encounter = DATA_VISITS_TO_ENCOUNTER_LABELS[row['VISIT']]
        else:
            encounter = ""
    else:
        encounter = f"VISIT not in row"
    return encounter

def get_bc_label(test_label):
    bc_label = ""
    if test_label in DATA_LABELS_TO_BC_LABELS: 
        bc_label = DATA_LABELS_TO_BC_LABELS[test_label]
    else:
        print("Add bc_label:",test_label)
    return bc_label

def get_data_contract(encounter,bc_label,property_name,tpt):
    if tpt == "":
        dc_item = next((item for item in data_contracts if item["BC_LABEL"] == bc_label and item['BCP_NAME'] == property_name and item['ENCOUNTER_LABEL'] == encounter), None)
    else:
        dc_item = next((item for item in data_contracts if item["BC_LABEL"] == bc_label and item['BCP_NAME'] == property_name and item['ENCOUNTER_LABEL'] == encounter and item['TIMEPOINT_VALUE'] == tpt), None)

    if dc_item != None:
        matches.append(dc_item)
        if "DC_URI" in dc_item:
            return dc_item['DC_URI']
        else:
            print("Missing DC_URI", encounter, bc_label)
    else:
        # add_issue("get_data_contract Miss BC_LABEL:", bc_label, "BCP_NAME:",property_name, "ENCOUNTER_LABEL:",encounter,"TPT:",tpt)
        return None

def get_data_contract_dm(encounter,bc_label,property_name):
    dc_item = next((item for item in data_contracts if item["BC_LABEL"] == bc_label and item['BCP_NAME'] == property_name and item['ENCOUNTER_LABEL'] == encounter), None)
    if dc_item != None:
        matches.append(dc_item)
        if "DC_URI" in dc_item:
            return dc_item['DC_URI']
        else:
            print("Missing DC_URI", bc_label, property_name)
    else:
        add_issue("get_data_contract_dm Miss BC_LABEL:", bc_label, "BCP_NAME:",property_name)
        return None

def get_data_contract_ae(bc_label,property_name):
    dc_item = next((item for item in data_contracts if item["BC_LABEL"] == bc_label and item['BCP_NAME'] == property_name), None)
    if dc_item != None:
        matches.append(dc_item)
        if "DC_URI" in dc_item:
            # return dc_item['DC_URI']+f"/{usubjid}/{seq}"
            return dc_item['DC_URI']
        else:
            print("Missing DC_URI", bc_label, property_name)
            return None
    else:
        add_issue("get_data_contract_dm Miss BC_LABEL:", bc_label, "BCP_NAME:",property_name)
        return None

def get_vs_variable(data, row, data_property, sdtm_variable):
    item = {}
    encounter = get_encounter(row)
    if encounter != "":
        bc_label = get_bc_label(row['VSTEST'])
        tpt = ""
        if 'VSTPT' in row and row['VSTPT'] != "":
            tpt = DATA_TPT_TO_TIMING_LABELS[row['VSTPT']]

        property = get_property_for_variable(row['VSTEST'],data_property)
        data_contract = get_data_contract(encounter,bc_label,property,tpt)
        if data_contract:
            if row[sdtm_variable]:
                item['USUBJID'] = row['USUBJID']
                item['DC_URI'] = data_contract
                item['DATAPOINT_URI'] = f"{data_contract}/{row['USUBJID']}/{row['VSSEQ']}"
                item['VALUE'] = f"{row[sdtm_variable]}"
                data.append(item)
                add_row_dp('VS',['USUBJID','VSSEQ'], row, item['DATAPOINT_URI'])
        else:
            # add_issue(f"No dc RESULT bc_label: {bc_label} - property: {property} - encounter: {encounter}")
            add_issue(f"Ignoring {data_property} for:", bc_label)
    else:
            add_issue("Ignoring visit", row['VISIT'], "encounter:", encounter)

def get_vs_data(data):
    print("\nGetting VS data")
    VS_DATA = Path.cwd() / "data" / "input" / "vs.json"
    assert VS_DATA.exists(), "VS_DATA not found"
    with open(VS_DATA) as f:
        vs_data = json.load(f)

    for row in vs_data:
        add_row_dp('VS',['USUBJID','VSSEQ'], row)
        get_vs_variable(data, row, 'VSORRES', 'VSORRES')
        get_vs_variable(data, row, 'date', 'VSDTC')
        get_vs_variable(data, row, 'position', 'VSPOS')
        get_vs_variable(data, row, 'location', 'VSLOC')


def get_lb_variable(data, row, data_property, sdtm_variable):
    item = {}
    encounter = get_encounter(row)
    if encounter != "":
        bc_label = get_bc_label(row['LBTEST'])
        tpt = ""
        if 'LBTPT' in row and row['LBTPT'] != "":
            tpt = DATA_TPT_TO_TIMING_LABELS[row['LBTPT']]

        property = get_property_for_variable(row['LBTEST'],data_property)
        if property:
            data_contract = get_data_contract(encounter,bc_label,property,tpt)
            if data_contract:
                if row[sdtm_variable]:
                    item['USUBJID'] = row['USUBJID']
                    item['DC_URI'] = data_contract
                    item['DATAPOINT_URI'] = f"{data_contract}/{row['USUBJID']}/{row['LBSEQ']}"
                    item['VALUE'] = f"{row[sdtm_variable]}"
                    data.append(item)
                    add_row_dp('LB',['USUBJID','LBSEQ'], row, item['DATAPOINT_URI'])
            else:
                add_issue(f"No dc RESULT bc_label: {bc_label} - property: {property} - encounter: {encounter}")
        else:
            add_issue("Add property for LBTEST",row['LBTEST'],"LBORRESU",row['LBORRESU'])
    else:
            add_issue("Ignoring visit", row['VISIT'], "encounter:", encounter)

def get_lb_data(data):
    print("\nGetting LB data")
    LB_DATA = Path.cwd() / "data" / "input" / "lb.json"
    assert LB_DATA.exists(), "LB_DATA not found"
    with open(LB_DATA) as f:
        lb_data = json.load(f)

    for row in lb_data:
        add_row_dp('LB',['USUBJID','LBSEQ'], row)
        get_lb_variable(data, row, 'LBORRES', 'LBORRES')
        get_lb_variable(data, row, 'LBORRESU', 'LBORRESU')
        get_lb_variable(data, row, 'date', 'LBDTC')

def get_bc_properties_dm( bc_label):
    query = f"""
        MATCH (bc:BiomedicalConcept)-[:PROPERTIES_REL]->(bcp)<-[:PROPERTIES_REL]-(dc:DataContract)-[:INSTANCES_REL]->(act_inst)-[:ENCOUNTER_REL]-(enc)
        WHERE bc.label in {bc_label}
        return bc.label as BC_LABEL, bcp.name as BCP_NAME, bcp.label as BCP_LABEL, enc.label as ENCOUNTER_LABEL, dc.uri as DC_URI
    """
    results = db_query(query)
    if results == []:
        return []
    return results

def get_dm_variable(data, row, data_label, data_property, sdtm_variable):
    # DM does not contain VISIT
    # dm_visit ='SCREENING 1'
    # bc_label = "Sex"
    dm_visit = "Screening 1"
    item = {}
    bc_label = get_bc_label(data_label)
    property_name = get_property_for_variable(bc_label,data_property)
    data_contract = get_data_contract_dm(dm_visit,bc_label,property_name)
    if property_name:
        if data_contract:
            if row[sdtm_variable]:
                item['USUBJID'] = row['USUBJID']
                item['DC_URI'] = data_contract
                item['DATAPOINT_URI'] = f"{data_contract}/{row['USUBJID']}"
                item['VALUE'] = f"{row[sdtm_variable]}"
                data.append(item)
                add_row_dp('DM',['USUBJID'], row, item['DATAPOINT_URI'])
                # Faking multipe race for one subject
                if row['USUBJID'] == '01-701-1028' and data_label == 'Race':
                    item2 = {}
                    item2['USUBJID'] = row['USUBJID']
                    item2['DC_URI'] = data_contract
                    item2['DATAPOINT_URI'] = f"{data_contract}/{row['USUBJID']}"
                    item2['VALUE'] = "ASIAN"
                    data.append(item2)
                    add_row_dp('DM',['USUBJID'], row, item['DATAPOINT_URI']+"race_supp")
        else:
            add_issue(f"No dc RESULT bc_label: {bc_label} - property_name: {property_name} - encounter: {dm_visit}")
    else:
        add_issue("Add property_name for DM",data_label,'value',row[sdtm_variable])

def add_dm_variable(dc, subject, value, xtra = None):
    if xtra:
        dp = f"{dc}/{subject}/{xtra}"
    else:
        dp = f"{dc}/{subject}"
    
    return dp

data_load = []

def add_datapoint(dc, data):
    item = {}
    item['USUBJID'] = data['USUBJID']
    item['DC_URI'] = data['DC_URI']
    item['DATAPOINT_URI'] = data['DATAPOINT_URI']
    data_load.append

def get_dm_data(data):
    print("-- Converting DM data")
    print("-- Get data contracts")
    properties = get_bc_properties_dm(['Sex', 'Race','Informed Consent Obtained'])
    for x in properties:
        debug.append(x)

    # Sex
    SEX_DC = next((p['DC_URI'] for p in properties if p['BC_LABEL'] == 'Sex' and p['BCP_NAME'] == 'Sex'), None)
    values = [{'s':r['USUBJID'],'v':r['SEX']} for r in data]
    for v in values:
        debug.append(v)
        data_load.append()
    # for row in data:
    #     datapoints = []
    #     add_row_dp('DM',['USUBJID'],row)
    #     data_load.append(add_dm_variable(SEX_DC,row['USUBJID'],row['SEX']))
    #     datapoints.append(datapoint_uri)

    # DTC_URI = next((p['DC_URI'] for p in properties if p['BC_LABEL'] == 'Sex' and p['BCP_NAME'] == '--DTC'), None)


    # for row in data:
    #     get_dm_variable(data, row, 'Sex', 'value', 'SEX')
        # get_dm_variable(data, row, 'Race', 'value', 'RACE')
        # get_dm_variable(data, row, 'Informed Consent', 'value', 'RFICDTC')
        # # NB: Faking Informed consent date
        # get_dm_variable(data, row, 'Informed Consent', 'date', 'RFICDTC')
        # get_dm_variable(data, row, 'Date of Birth', 'value', 'BRTHDTC')
        # get_dm_variable(data, row, 'Sex', 'date', 'DMDTC')
        # # Ethnicity. No BC
   



def import_domains(domain_files):
    # OUTPUT_PATH = Path.cwd() / "data" / "output"
    # assert OUTPUT_PATH.exists(), "OUTPUT_PATH not found"
    clear_created_nodes()

    for file in domain_files:
        with open(file) as f:
            data = json.load(f)
        domain_name = Path(file).stem
        if domain_name == "dm":
            get_dm_data(data)
        else:
            print(f"Not handling {domain_name}")
        # if 

    write_tmp("step-20-load-data-configuration.txt",debug)

    return


    # Get subjects from the enrolment file
    subjects = [row['USUBJID'] for row in enrolment_data]


    print("\nCreating datapoint and value")
    data = []

    get_vs_data(data)
    get_lb_data(data)
    get_dm_data(data)
    get_ex_data(data)
    # Must fix data contracts for event driven
    get_ae_data(data)


    print("\n---Datapoint - Data contract matches:",len(matches))
    print("---Non matching Datapoints (e.g. visit not defined)",len(issues))
    print("\nIssues")
    if len(issues) == 0:
        print("None")
    for issue in set(issues):
        print(issue)
    print("")

    for k,v in collection_datapoints.items():
        debug.append(f"{k}-{v}")


    print("--- number of Datapoints:",len(data))
    if len(data) == 0:
        print("No data has been found")
        exit()

    save_file(OUTPUT_PATH,"datapoints",data)
    # check_dc_in_file(OUTPUT_PATH,"datapoints")

    # row datapoints
    print("\nCreating datapoints relation to row")
    for_csv = []
    for key,dps in collection_datapoints.items():
        for dp in dps:
            item = {}
            item["key"] = key
            item['datapoint_uri'] = dp
            for_csv.append(item)
    output_csv(OUTPUT_PATH,"collection_datapoints.csv",for_csv)
    output_json(OUTPUT_PATH,"collection_datapoints",collection_datapoints)
    # save_file(OUTPUT_PATH,"collection_datapoints",collection_datapoints)


    print("\ndone")

if __name__ == "__main__":
    DM_DATA = Path.cwd() / "data" / "input" / "dm.json"
    domain_files = [DM_DATA]
    import_domains(domain_files)