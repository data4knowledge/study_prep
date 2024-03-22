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

def to_debug(*txts):
    list = []
    for x in txts:
        list.append(x)
    debug.append(" ".join(list))

