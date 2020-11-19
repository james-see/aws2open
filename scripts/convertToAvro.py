import os
import json

import numpy as np
import pandas as pd
import pandavro as pdx

chd = os.chdir(f"{os.environ['HOME']}/projects/aws2open/")
OUTPUT_PATH=f"{chd}keys.avro"

def main():
    cooljson = list()
    with open('json/keys.json') as f:
        d = json.load(f)
    # print(d)
    for k,v in d.items():
        if isinstance(v, dict):
            print(v)
            for nestedk, nestedv in v.items():
                if isinstance(nestedv, list):
                    for item in nestedv:
                        print(item)
                        cooljson.append({k: str(item)})
    print(cooljson)
    mydf = pd.json_normalize(cooljson)
    print(mydf.head(3))
    # mydf = pd.read_json("json/keys.json", orient='records', dtype='dict')
    # pdx.to_avro(OUTPUT_PATH, mydf)
    # saved = pdx.read_avro(OUTPUT_PATH)
    # print(saved)


if __name__ == '__main__':
    main()
