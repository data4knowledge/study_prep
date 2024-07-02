import json
import csv
from pathlib import Path
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

DATA_CONTRACTS_LOOKUP = Path.cwd() / "data" / "output" / "data_contracts.json"
assert DATA_CONTRACTS_LOOKUP.exists(), "DATA_CONTRACTS_LOOKUP not found"
print("\nGetting data contracts from file",DATA_CONTRACTS_LOOKUP)
with open(DATA_CONTRACTS_LOOKUP) as f:
    data_contracts = json.load(f)

def add_issue(*txts):
    add = []
    for txt in txts:
        if txt == None:
            add.append("None")
        else:
            add.append(txt)
    issues.append(" ".join(add))


def save_file(path: Path, name, data):
    OUTPUT_FILE = path / f"{name}.json"
    print("Saving to",OUTPUT_FILE)
    with open(OUTPUT_FILE, 'w') as f:
        f.write(json.dumps(data, indent = 2))

    OUTPUT_FILE = path / f"{name}.csv"
    print("Saving to",OUTPUT_FILE)
    output_variables = list(data[0].keys())
    with open(OUTPUT_FILE, 'w') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=output_variables)
        writer.writeheader()
        writer.writerows(data)


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
        print("Add property",test,variable)
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

def get_data_contract(encounter,bc_label,property,tpt):
    if tpt == "":
        dc_item = next((item for item in data_contracts if item["BC_LABEL"] == bc_label and item['BCP_NAME'] == property and item['ENCOUNTER_LABEL'] == encounter), None)
    else:
        dc_item = next((item for item in data_contracts if item["BC_LABEL"] == bc_label and item['BCP_NAME'] == property and item['ENCOUNTER_LABEL'] == encounter and item['TIMEPOINT_VALUE'] == tpt), None)

    if dc_item != None:
        matches.append(dc_item)
        if "DC_URI" in dc_item:
            return dc_item['DC_URI']
        else:
            print("Missing DC_URI", encounter, bc_label)
    else:
        add_issue("get_data_contract Miss BC_LABEL:", bc_label, "BCP_LABEL:",property, "ENCOUNTER_LABEL:",encounter,"TPT:",tpt)
        return None

def get_data_contract_dm(encounter,bc_label,property):
    dc_item = next((item for item in data_contracts if item["BC_LABEL"] == bc_label and item['BCP_NAME'] == property and item['ENCOUNTER_LABEL'] == encounter), None)
    if dc_item != None:
        matches.append(dc_item)
        if "DC_URI" in dc_item:
            return dc_item['DC_URI']
        else:
            print("Missing DC_URI", bc_label, property)
    else:
        add_issue("get_data_contract_dm Miss BC_LABEL:", bc_label, "BCP_LABEL:",property)
        add_issue("-- Miss BC_LABEL:", bc_label, "BCP_LABEL:",property)
        return None

def create_subject_data_load_file():
    ENROLMENT_DATA = Path.cwd() / "data" / "output" / "enrolment.json"
    assert ENROLMENT_DATA.exists(), "ENROLMENT_DATA not found"
    # print("\nGetting subjects from file",ENROLMENT_DATA)
    with open(ENROLMENT_DATA) as f:
        enrolment_data = json.load(f)

    OUTPUT_PATH = Path.cwd() / "data" / "output"
    assert OUTPUT_PATH.exists(), "OUTPUT_PATH not found"

    # Get subjects from the enrolment file
    subjects = [row['USUBJID'] for row in enrolment_data]


    print("\nCreating datapoint and value")
    data = []

    # print("\nGetting VS data")
    # VS_DATA = Path.cwd() / "data" / "input" / "vs.json"
    # assert VS_DATA.exists(), "VS_DATA not found"
    # with open(VS_DATA) as f:
    #     vs_data = json.load(f)


    # for row in vs_data:
    #     item = {}

    #     # Result
    #     encounter = get_encounter(row)
    #     if encounter != "":
    #         bc_label = get_bc_label(row['VSTEST'])
    #         tpt = ""
    #         if 'VSTPT' in row and row['VSTPT'] != "":
    #             tpt = DATA_TPT_TO_TIMING_LABELS[row['VSTPT']]

    #         property = get_property_for_variable(row['VSTEST'],'VSORRES')

    #         data_contract = get_data_contract(encounter,bc_label,property,tpt)

    #         if data_contract:
    #             item['USUBJID'] = row['USUBJID']
    #             item['DC_URI'] = data_contract
    #             item['DATAPOINT_URI'] = f"{data_contract}/{row['USUBJID']}"
    #             item['VALUE'] = f"{row['VSORRES']}"
    #             data.append(item)
    #         else:
    #             add_issue(f"No dc RESULT bc_label: {bc_label} - property: {property} - encounter: {encounter}")

    #         # Unit
    #         encounter = get_encounter(row)
    #         bc_label = get_bc_label(row['VSTEST'])
    #         property = get_property_for_variable(row['VSTEST'],'VSORRESU')
    #         data_contract = get_data_contract(encounter,bc_label,property,tpt)
    #         if data_contract:
    #             item = {}
    #             item['USUBJID'] = row['USUBJID']
    #             item['DC_URI'] = data_contract
    #             item['DATAPOINT_URI'] = f"{data_contract}/{row['USUBJID']}"
    #             item['VALUE'] = f"{row['VSORRESU']}"
    #             data.append(item)
    #         else:
    #             add_issue("No dc UNIT bc_label:", bc_label, "- encounter:", encounter, "property:", property)
    #     else:
    #             add_issue("Ignoring visit", row['VISIT'], "encounter:", encounter)
            

    # print("\nGetting LB data")
    # LB_DATA = Path.cwd() / "data" / "input" / "lb.json"
    # assert LB_DATA.exists(), "LB_DATA not found"
    # with open(LB_DATA) as f:
    #     lb_data = json.load(f)

    # for row in lb_data:
    #     item = {}

    #     # Result
    #     encounter = get_encounter(row)
    #     if encounter != "":
    #         bc_label = get_bc_label(row['LBTEST'])
    #         tpt = ""
    #         if 'LBTPT' in row and row['LBTPT'] != "":
    #             tpt = DATA_TPT_TO_TIMING_LABELS[row['LBTPT']]

    #         property = get_property_for_variable(row['LBTEST'],'LBORRES')
    #         if property:
    #             data_contract = get_data_contract(encounter,bc_label,property,tpt)

    #             if data_contract:
    #                 item['USUBJID'] = row['USUBJID']
    #                 item['DC_URI'] = data_contract
    #                 item['DATAPOINT_URI'] = f"{data_contract}/{row['USUBJID']}"
    #                 item['VALUE'] = f"{row['LBORRES']}"
    #                 data.append(item)
    #             else:
    #                 add_issue(f"No dc RESULT bc_label: {bc_label} - property: {property} - encounter: {encounter}")
    #         else:
    #             add_issue("Add property for LBTEST",row['LBTEST'],"LBORRESU",row['LBORRESU'])
    #         # Unit
    #         encounter = get_encounter(row)
    #         bc_label = get_bc_label(row['LBTEST'])
    #         property = get_property_for_variable(row['LBTEST'],'LBORRESU')
    #         if property:
    #             data_contract = get_data_contract(encounter,bc_label,property,tpt)
    #             if data_contract:
    #                 item = {}
    #                 item['USUBJID'] = row['USUBJID']
    #                 item['DC_URI'] = data_contract
    #                 item['DATAPOINT_URI'] = f"{data_contract}/{row['USUBJID']}"
    #                 item['VALUE'] = f"{row['LBORRESU']}"
    #                 data.append(item)
    #             else:
    #                 add_issue("No dc UNIT bc_label:", bc_label, "- encounter:", encounter, "property:", property)
    #         else:
    #             add_issue("Add property for LBTEST",row['LBTEST'],"LBORRESU",row['LBORRESU'])
    #     else:
    #             add_issue("Ignoring visit", row['VISIT'], "encounter:", encounter)


    # print("\nGetting DM data")
    DM_DATA = Path.cwd() / "data" / "input" / "dm.json"
    assert DM_DATA.exists(), "DM_DATA not found"
    with open(DM_DATA) as f:
        dm_data = json.load(f)

    # # DM does not contain VISIT
    # # Sex
    # for row in dm_data:
    #     item = {}

    #     dm_visit = "Screening 1"
    #     bc_label = get_bc_label("Sex")
    #     property = get_property_for_variable(bc_label,'value')
    #     data_contract = get_data_contract_dm(dm_visit,bc_label,property)
    #     if property:
    #         if data_contract:
    #             item['USUBJID'] = row['USUBJID']
    #             item['DC_URI'] = data_contract
    #             item['DATAPOINT_URI'] = f"{data_contract}/{row['USUBJID']}"
    #             item['VALUE'] = f"{row['SEX']}"
    #             data.append(item)
    #         else:
    #             add_issue(f"No dc RESULT bc_label: {bc_label} - property: {property} - encounter: {dm_visit}")
    #     else:
    #         add_issue("Add property for DM","Sex",'value',row['SEX'])

    # Race
    for row in dm_data:
        item = {}

        dm_visit = "Screening 1"
        bc_label = get_bc_label("Race")
        # print("bc_label",bc_label)
        property_name = get_property_for_variable(bc_label,'value')
        # print("  bc_property",property_name)
        data_contract = get_data_contract_dm(dm_visit,bc_label,property_name)
        # print("    dc_uri",data_contract)
        if property_name:
            if data_contract:
                item['USUBJID'] = row['USUBJID']
                item['DC_URI'] = data_contract
                item['DATAPOINT_URI'] = f"{data_contract}/{row['USUBJID']}"
                item['VALUE'] = f"{row['RACE']}"
                data.append(item)
            else:
                add_issue(f"No dc RESULT bc_label: {bc_label} - property: {property_name} - encounter: {dm_visit}")
        else:
            add_issue("Add property for DM","Race",'value',row['RACE'])

    # Informed Consent
    for row in dm_data:
        item = {}

        dm_visit = "Screening 1"
        bc_label = get_bc_label("Informed Consent")
        # print("bc_label",bc_label)
        property_name = get_property_for_variable(bc_label,'value')
        # print("  property_name",property_name)
        data_contract = get_data_contract_dm(dm_visit,bc_label,property_name)
        # print("    data_contract",data_contract)
        if property_name:
            if data_contract:
                # print("creating data contract Informed Consent (Obtained)")
                item['USUBJID'] = row['USUBJID']
                item['DC_URI'] = data_contract
                item['DATAPOINT_URI'] = f"{data_contract}/{row['USUBJID']}"
                item['VALUE'] = f"{row['RFICDTC']}"
                data.append(item)

                # # also add dsdecod
                # property_name = get_property_for_variable(bc_label,'decod')
                # # print("decod property_name",property_name)
                # data_contract = get_data_contract_dm(dm_visit,bc_label,property_name)
                # # print("decod data_contract",data_contract)
                # item['USUBJID'] = row['USUBJID']
                # item['DC_URI'] = data_contract
                # item['DATAPOINT_URI'] = f"{data_contract}/{row['USUBJID']}"
                # item['VALUE'] = f"Informed Consent Obtained"
                # data.append(item)
            else:
                add_issue(f"No dc RESULT bc_label: {bc_label} - property_name: {property_name} - encounter: {dm_visit}")
        else:
            add_issue("Add property_name for DM","Informed Consent",'value',row['RFICDTC'])

    # Date of Birth
    for row in dm_data:
        item = {}

        dm_visit = "Screening 1"
        bc_label = get_bc_label("Date of Birth")
        property_name = get_property_for_variable(bc_label,'value')
        # debug.append(f"\nbc_label {bc_label} -> {property_name}")
        data_contract = get_data_contract_dm(dm_visit,bc_label,property_name)
        # debug.append("DC_CONTRACT:"+data_contract)
        # debug.append("Should be  "+"https://study.d4k.dk/study-cdisc-pilot-lzzt/FAKE_UUIDDateofBirth/1a3e81d0-5534-446e-86d4-5aea6455ded5")
        # debug.append(f"bc_label+prop {property_name}  -> {data_contract}")
        if property_name:
            if data_contract:
                item['USUBJID'] = row['USUBJID']
                item['DC_URI'] = data_contract
                item['DATAPOINT_URI'] = f"{data_contract}/{row['USUBJID']}"
                item['VALUE'] = f"{row['BRTHDTC']}"
                data.append(item)
            else:
                add_issue(f"No dc RESULT bc_label: {bc_label} - property_name: {property_name} - encounter: {dm_visit}")
        else:
            add_issue("Add property_name for DM","Date of Birth",'value',row['BRTHDTC'])


    print("---Datapoint - Data contract matches:",len(matches))
    print("---Non matching Datapoints (e.g. visit not defined)",len(issues))
    print("\nIssues")
    for issue in set(issues):
        print(issue)
    print("")

    print("--- number of Datapoints:",len(data))
    if len(data) == 0:
        print("No data has been found")
        exit()

    save_file(OUTPUT_PATH,"datapoints",data)

    write_tmp("step-2-dc-debug.txt",debug)

    print("\ndone")

if __name__ == "__main__":
    create_subject_data_load_file()
