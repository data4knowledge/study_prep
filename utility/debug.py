import json
import pandas as pd
from pathlib import Path

debug = []

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
    print("Writing debug...",OUTPUT_FILE, end="")
    with open(OUTPUT_FILE, 'w') as f:
        for it in data:
            f.write(str(it))
            f.write('\n')
    print(" ...done")


def get_xpt_data(file):
    df = pd.read_sas(file,format='xport',encoding='ISO-8859-1')
    df = df.fillna('')
    data = df.to_dict(orient='records')
    return data


def to_debug(*txts):
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

# check_lb()
