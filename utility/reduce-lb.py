import json
from pathlib import Path

LABELS = [
    "Alanine Aminotransferase",
    "Albumin",
    "Alkaline Phosphatase",
    "Aspartate Aminotransferase",
    "Creatinine",
    "Potassium",
    "Sodium"
]


def get_data(file):
    with open(file, 'r') as file:
        header = file.readline().strip().split("\t")
        data = []
        for line in file.readlines():
            values = line.strip().split("\t")
            x = dict(map(lambda i,j : (i,j) , header,values))
            data.append(x)
    return data

DM_DATA = Path.cwd() / "data" / "dm.tsv"
assert DM_DATA.exists(), "SDTM_DATA not found"

dm = get_data(DM_DATA)

# subjects = [x['USUBJID'] for x in dm][0:10]
subjects = [x['USUBJID'] for x in dm][0:3]
print("subjects",subjects)

LB_DATA_FULL = Path.cwd() / "data" / "lb-full.tsv"
assert LB_DATA_FULL.exists(), "SDTM_DATA not found"

lb = get_data(LB_DATA_FULL)
print("len(lb)",len(lb))
lb_data = [x for x in lb if x['USUBJID'] in subjects and x['LBTEST'] in LABELS]
print("len(lb_data)",len(lb_data))

LB_DATA = Path.cwd() / "data" / "lb.json"
with open(LB_DATA, 'w') as f:
    f.write(json.dumps(lb_data, indent = 2))


LB_DATA = Path.cwd() / "data" / "lb.tsv"
with open(LB_DATA, 'w') as file:
    i = 0
    for x in lb_data:
        if i == 0:
            s = '\t'.join(list(x.keys()))
            file.write(s)
            file.write("\n")
        s = '\t'.join(list(x.values()))
        file.write(s)
        file.write("\n")
        i = i + 1
