from pathlib import Path
from debug import write_debug, write_tmp
from neo_utils import db_is_down
from d4kms_service import Neo4jConnection
from mappings import DATA_LABELS_TO_BC_LABELS, DATA_VISITS_TO_ENCOUNTER_LABELS, DATA_TPT_TO_TIMING_LABELS, TEST_ROW_VARIABLE_TO_BC_PROPERTY


def exist_query(query,value):
    db = Neo4jConnection()
    results = db.query(query)
    db.close()
    if results == None:
        print('query did not work"',query)
        exit()
    return [x.data() for x in results]

def check_bc_labels(ok, missing):
    for data_label, bc_label in DATA_LABELS_TO_BC_LABELS.items():
        query = f"""
            MATCH (bc:BiomedicalConcept {{label: '{bc_label}'}})
            RETURN count(bc) > 0 as result
        """
        result = exist_query(query, bc_label)
        if result[0]['result']:
            ok.append(["bc_label",bc_label])
        else:
            missing.append(["bc_label",bc_label])


def check_bc_properties(ok,missing):
    bc_properties = set()
    for test, items in TEST_ROW_VARIABLE_TO_BC_PROPERTY.items():
        for data_var, bc_property in items.items():
            bc_properties.add(bc_property)
    for bc_property in bc_properties:
        query = f"""
            MATCH (t:BiomedicalConceptProperty {{label: '{bc_property}'}})
            RETURN count(t) > 0 as result
        """
        result = exist_query(query, bc_property)
        if result[0]['result']:
            ok.append(["bc_property",bc_property])
        else:
            missing.append(["bc_property",bc_property])


def check_encounter_labels(ok,missing):
    for data_visit, encounter_label in DATA_VISITS_TO_ENCOUNTER_LABELS.items():
        query = f"""
            MATCH (enc:Encounter {{label: '{encounter_label}'}})
            RETURN count(enc) > 0 as result
        """
        result = exist_query(query, encounter_label)
        if result[0]['result']:
            ok.append(["encounter_label",encounter_label])
        else:
            missing.append(["encounter_label",encounter_label])


def check_timing_labels(ok,missing):
    for data_tpt, timing_label in DATA_TPT_TO_TIMING_LABELS.items():
        query = f"""
            MATCH (t:Timing {{value: '{timing_label}'}})
            RETURN count(t) > 0 as result
        """
        result = exist_query(query, timing_label)
        if result[0]['result']:
            ok.append(["timing_label",timing_label])
        else:
            missing.append(["timing_label",timing_label])


def main():
    if db_is_down():
        return "Neo4j not running"
    ok = []
    missing = []
    check_bc_labels(ok, missing)
    check_encounter_labels(ok,missing)
    check_timing_labels(ok,missing)
    check_bc_properties(ok,missing)
    # for o in ok:
    #     print(o)
    print("bc_label ok:", len([x for x in ok if x[0] == "bc_label"]), "missing:",len([x for x in missing if x[0] == "bc_label"]))
    print("encounter_label ok:", len([x for x in ok if x[0] == "encounter_label"]), "missing:",len([x for x in missing if x[0] == "encounter_label"]))
    print("timing_label ok:", len([x for x in ok if x[0] == "timing_label"]), "missing:",len([x for x in missing if x[0] == "timing_label"]))
    print("bc_property ok:", len([x for x in ok if x[0] == "bc_property"]), "missing:",len([x for x in missing if x[0] == "bc_property"]))
    print("Missing ----")
    for m in missing:
        print(m)

if __name__ == "__main__":
    main()
