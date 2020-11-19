import os
import json

import numpy as np
import pandas as pd
import pandavro as pdx

chd = os.chdir(f"{os.environ['HOME']}/projects/aws2open/")
OUTPUT_PATH=f"{os.environ['HOME']}/projects/aws2open/avro/keys.avro"

def main():
    cooljson = list()
    with open('json/keys.json') as f:
        d = json.load(f)
    # print(d)
    for k in d:
        if isinstance(k, dict):
            # print(k)
            for somek, somev in k.items():
                if isinstance(somev, dict):
                    for ik, iv in somev.items():
                        if isinstance(iv, list):
                            for listitem in iv:
                                for nestedk, nestedv in listitem.items():
                                    print(nestedv)
                                    cooljson.append({ik: str(nestedv)})
    print(cooljson)
    mydf = pd.DataFrame.from_records(cooljson)
    print(mydf.head(10))
    mydffixed = mydf.replace(np.nan, '', regex=True)
    # mydf = pd.read_json("json/keys.json", orient='records', dtype='dict')
    pdx.to_avro(OUTPUT_PATH, mydffixed)
    saved = pdx.read_avro(OUTPUT_PATH)
    print(saved)


if __name__ == '__main__':
    main()
