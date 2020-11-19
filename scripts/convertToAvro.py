import os
import json

import numpy as np
import pandas as pd
import pandavro as pdx

# set the project directory to where this code resides after cloning
projectdir = f"{os.environ['HOME']}/projects/aws2open/"

chd = os.chdir(projectdir)
OUTPUT_PATH = f"{projectdir}avro/keys.avro"


def main():
    """
    Takes the keys.json file and iterates through it to write out
    a flattened list of dicts and then that turns into a Pandas DataFrame
    that then gets converted to Avro and saved to the avro folder from pandavro
    """

    cooljson = list()
    with open("json/keys.json") as f:
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
                                    # print(nestedv)
                                    cooljson.append({ik: str(nestedv)})
    # print(cooljson)
    mydf = pd.DataFrame.from_records(cooljson)
    mydffixed = mydf.replace(np.nan, "", regex=True)  # get rid of the NaNs!
    # mydf = pd.read_json("json/keys.json", orient='records', dtype='dict')
    pdx.to_avro(OUTPUT_PATH, mydffixed)
    # saved = pdx.read_avro(OUTPUT_PATH)
    # print(saved)


if __name__ == "__main__":
    main()
