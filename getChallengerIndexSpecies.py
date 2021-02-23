#!/usr/bin/env python3
import subprocess
import numpy as np
import random
import pandas as pd
import os
import requests
import json
import re
import datetime
import sys
import time
import io
import pickle

rootPath = "/home/mwiecksosa/oceans1876/"
dataPath = rootPath+"data/"

def main():

    # what I used to get the index text
    """
    f = dataPath+'part2OCR.txt'
    fString = io.open(f, mode="r", encoding="utf-8")
    text = fString.read()
    beginIndexSplit = "the species are fully treated of."
    endIndexSplit = "1608 THE VOYAGE OF H.M.S. CHALLENGER."
    indexSpecies = text.split(endIndexSplit)[0].split(beginIndexSplit)[1]

    with open(dataPath+"index.txt", "w") as text_file:
        text_file.write(indexSpecies)
    """

    f = dataPath+'index.txt'

    with open(f,'r') as f:
        dataList = []
        speciesName = ""
        speciesPage = ""
        genusName = ""
        genusPage = ""
        for i,line in enumerate(f):
            print(line)
            try:
                if line[0].isupper():
                    genusName = line.split(",")[0].split(" ")[0]
                    try:
                        genusPage = line.split(",")[1]
                    except:
                        genusPage = "NA"
                elif line[0].islower():
                    speciesName = genusName + " " + line.split(",")[0]
                    try:
                        speciesPage = line.split(",")[1]
                    except:
                        speciesPage = "NA"
            except Exception as e:
                print(e)
                speciesName=""
                speciesPage=""
                genusName=""
                genusPage=""

            dataList.append([speciesName, speciesPage, genusName, genusPage])

        colnames = ["speciesName", "speciesPage", "genusName", "genusPage"]
        df = pd.DataFrame(columns=colnames,data=dataList)
        df.to_csv(dataPath+"indexSpeciesExtracted.csv")

if __name__ == '__main__':
    main()
