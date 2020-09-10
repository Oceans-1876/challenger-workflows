#!/usr/bin/env python3
import sys
import os
import requests
import time
import json
import numpy as np
import pandas as pd
import re

rootPath = "/home/mwiecksosa/oceans1876/"
dataPath = rootPath+"data/"

def main():
    fileNameList = ['part1OCR.txt','part2OCR.txt']

    for fn in fileNameList:
        f = dataPath+fn
        getSpeciesNames(fn=fn,f=f)
        getStationOfSpecies(fn=fn,f=f)

def getSpeciesNames(fn,f):
    """ Parse species names from text then verify names using databases """
    parseSpeciesNamesGNRD(fn=fn,f=f)
    verifySpeciesNamesGNI(fn=fn,f=f)

    # no need to merge since not using the offsetStart offsetEnd anymore
    # ==> verified DF has all the info
    # mergeDataframes(fn=fn,f=f)

def getStationOfSpecies(fn,f):
    """ If species name is in a certain line, assign correct station to it """

    assignStationNumberToEachLine(fn=fn,f=f)

    ### WARNING: findLinesOfSpeciesUsingOffsetStartEnd is broken
    # Need to fix the byte offset from the global names programs
    # figure out what this does:
    # https://github.com/gnames/gnfinder/blob/6394fa1cb0b08be57737f5597cdb8e267a05d21c/token/tokenize.go
    # CTRL+F "start"
    # tokenize.go uses token data structure which is used for offsetStart, offsetEnd
    # how to get bytes or line+column from offsetStart and offsetEnd?
    # messaged developer on github and he said it's just UTF-8, try this again
    findLinesOfSpeciesByteOffset(fn=fn,f=f)

    ### WARNING: very slow (takes hours)... don't use for 50 volumes
    # use findLinesOfSpeciesUsingOffsetStartEnd once
    # we figure out how to use the offsetStart and offsetEnd to find species
    # namke in the text file
    findLinesOfSpeciesByParsing(fn=fn,f=f)

    # each station with species that were observed at that station
    # one station per line, list of species
    assignSpeciesToStations(fn=fn,f=f)

    # each species with the stations it was observed at
    # one species per line, list of stations
    assignStationNumberToSpecies(fn=fn,f=f)

def assignSpeciesToStations(fn,f):
    """ Assign species list to each station based on lines species name is in.
    Create a dictionary of the species and a list of lines they are in.
    Iterate through stations and if species between station startLine & endLine
    then append that speciesName to the list of species at that station. """

    print("Assigning species list to stations...")

    loadNameDfSpeciesLines = dataPath+"dfSpeciesLines"+fn[:-4]+".csv"
    loadNameDfStationLines = dataPath+"dfStationLines"+fn[:-4]+".csv"

    dfSpeciesLines = pd.read_csv(loadNameDfSpeciesLines)
    dfStationLines = pd.read_csv(loadNameDfStationLines)

    dictSpeciesNameLineList = {}
    for i,row_dfSpeciesLines in dfSpeciesLines.iterrows():

        tempSpeciesNameInLines = row_dfSpeciesLines.appearsInLine.split(",")

        # speciesNameInLines: list of lines where species name is seen
        speciesNameInLines = []
        for ele in tempSpeciesNameInLines:
            if '[' in ele:
                ele = ele.replace('[','')
            if ']' in ele:
                ele = ele.replace(']','')
            speciesNameInLines.append(ele)

        n = str(row_dfSpeciesLines.speciesName)
        dictSpeciesNameLineList[n] = speciesNameInLines

    speciesListForAllStations = []
    for i,row_dfStationLines in dfStationLines.iterrows():

        assignedSpeciesListForOneStation = []
        for key,val in dictSpeciesNameLineList.items():
            foundThisSpeciesAtThisStation = False
            for line in val:

                if line == '':
                    continue
                if int(line) >= int(row_dfStationLines.startLine) \
                and int(line) <= int(row_dfStationLines.endLine):
                    assignedSpeciesListForOneStation.append(key)
                    print("Found species! {}".format(key))
                    foundThisSpeciesAtThisStation = True
                if foundThisSpeciesAtThisStation: # then go to next species
                    break
        speciesListForAllStations.append(assignedSpeciesListForOneStation)


        print(speciesListForAllStations)

    dfStationLines['species'] = speciesListForAllStations

    saveName = dataPath+"dfStationsWithVerifiedSpeciesList"+fn[:-4]+".csv"

    if os.path.exists(saveName):
        os.remove(saveName)
    dfStationLines.to_csv(saveName)




def assignStationNumberToSpecies(fn,f):
    """ Assign station list to each species based on lines species name is in.
    Iterate through dfSpeciesLines and if appearsInLine in the line of a station
    according to dfStationLines then add that station to list of stations for
    that particular species and save as dfVerifiedSpeciesWithStationsList. """

    print("Assigning station numbers to species...")

    loadNameDfSpeciesLines = dataPath+"dfSpeciesLines"+fn[:-4]+".csv"
    loadNameDfStationLines = dataPath+"dfStationLines"+fn[:-4]+".csv"

    dfSpeciesLines = pd.read_csv(loadNameDfSpeciesLines)
    dfStationLines = pd.read_csv(loadNameDfStationLines)

    lastStationStartLine = 0
    speciesAssignedStationsList = []
    for i,row_dfSpeciesLines in dfSpeciesLines.iterrows():
        print("row_dfSpeciesLines.appearsInLine",row_dfSpeciesLines.appearsInLine)
        print(type(row_dfSpeciesLines.appearsInLine))

        tempSpeciesNameInLines = row_dfSpeciesLines.appearsInLine.split(",")
        print("tempSpeciesNameInLines",tempSpeciesNameInLines)
        speciesNameInLines = []
        for ele in tempSpeciesNameInLines:
            if '[' in ele:
                ele = ele.replace('[','')
            if ']' in ele:
                ele = ele.replace(']','')
            speciesNameInLines.append(ele)


        print("speciesNameInLines",speciesNameInLines)
        print(type(speciesNameInLines))

        assignedStationList = []

        if speciesNameInLines == ['']:
            assignedStationList.append("No Station")
        else:
            for line in speciesNameInLines:

                foundStation = False
                for j,row_dfStationLines in dfStationLines.iterrows():
                    print("line",line)
                    print("row_dfStationLines.startLine",row_dfStationLines.startLine)
                    print("row_dfStationLines.endLine",row_dfStationLines.endLine)
                    if int(line) >= int(row_dfStationLines.startLine) \
                    and int(line) <= int(row_dfStationLines.endLine):
                        assignedStationList.append(row_dfStationLines.station)
                        foundStation = True
                        print("Found station! {}".format(row_dfStationLines.station))
                if not foundStation:
                    assignedStationList.append("Unkown Station")
                    print("Unkown station!")

        speciesAssignedStationsList.append(assignedStationList)
        print(speciesAssignedStationsList)

    dfSpeciesLines['station'] = speciesAssignedStationsList

    saveName = dataPath+"dfVerifiedSpeciesWithStationsList"+fn[:-4]+".csv"

    if os.path.exists(saveName):
        os.remove(saveName)
    dfSpeciesLines.to_csv(saveName)


def mergeDataframes(fn,f):
    """ WARNING: Don't use this now because not using offsets """

    print("TODO")

    loadParsedNames = dataPath+"parsedSpeciesNames"+fn[:-4]+".csv"
    loadVerifiedNames = dataPath+"verifiedSpeciesNames"+fn[:-4]+".csv"

    dfParsedNames = pd.read_csv(loadParsedNames)
    dfVerifiedNames = pd.read_csv(loadVerifiedNames)

    dfMerged = pd.merge(dfParsedNames, dfVerifiedNames, on='speciesName')
    dfMerged = dfMerged[['speciesName','offsetStart','offsetEnd','is_known_name']]

    saveName = dataPath+"finalSpeciesNames"+fn[:-4]+".csv"
    if os.path.exists(saveName):
        os.remove(saveName)
    dfMerged.to_csv(saveName)



    """
    if os.path.exists(loadParsedNames):
      os.remove(loadParsedNames)
    else:
      print("The file does not exist")

    if os.path.exists(loadVerifiedNames):
      os.remove(loadVerifiedNames)
    else:
      print("The file does not exist")
    """

def assignStationNumberToEachLine(fn,f):
    """ If line is in section about station X, assign station X to that Line """

    print("Assigning station numbers to lines in {}...".format(fn))

    with open(f,'r') as fd:
        for i, l in enumerate(fd):
            pass
    lengthOfFile = i

    dfStationLines = pd.DataFrame(columns=["station","startLine"])
    with open(f,'r') as f:
        for i,line in enumerate(f):
            print("{}% Line {}/{}".format(100*round(i/lengthOfFile,2),i,lengthOfFile))
            print(line)
            # example: Station 16 (Sounding 60)
            if "(Sounding" in line:
                print("Found station!")
                dfStationLines = dfStationLines.append({'station':line\
                                                       .split("(Sounding")[0]\
                                                       ,'startLine':i\
                                                       }, ignore_index=True)



    endLineList = []
    for i,row in dfStationLines.iterrows():
        # if first line then skip to next line
        if i == 0:
            continue
        else:
            endLineList.append(row.startLine)
    # if at end of file then that Station ends at end of file
    endLineList.append(lengthOfFile)

    dfStationLines['endLine'] = endLineList

    saveName = dataPath+"dfStationLines"+fn[:-4]+".csv"
    if os.path.exists(saveName):
        os.remove(saveName)
    dfStationLines.to_csv(saveName)
    return dfStationLines

def findLinesOfSpeciesByParsing(fn,f):
    loadNameSpecies = dataPath+"verifiedSpeciesNames"+fn[:-4]+".csv"
    dfSpecies = pd.read_csv(loadNameSpecies)

    print("Getting lines of species names from {}...".format(fn))

    with open(f, encoding="utf8") as fd:
        for i, l in enumerate(fd):
            pass
    lengthOfFile = i + 1

    dfSpeciesLines = pd.DataFrame(columns=["speciesName","appearsInLine"])

    for i,speciesName in enumerate(dfSpecies['speciesName']):
        print("{}/{} Searching for {}...".format(i+1,len(dfSpecies),speciesName))

        inLines = []
        with open(f, 'r') as file:
            for j,line in enumerate(file):
                print("{}/{} Searching for {} on line {}/{}..."\
                .format(i+1,len(dfSpecies),speciesName,j,lengthOfFile))
                if speciesName in line:
                    inLines.append(j)
                    print("Found on this line!")
        dfSpeciesLines = dfSpeciesLines.append({"speciesName":speciesName,
                                                "appearsInLine":inLines
                                                }, ignore_index=True)

    saveName = dataPath+"dfSpeciesLines"+fn[:-4]+".csv"
    if os.path.exists(saveName):
        os.remove(saveName)
    dfSpeciesLines.to_csv(saveName)
    return dfSpeciesLines

def findLinesOfSpeciesByteOffset(fn,f):
    """ WARNING: Not working...
        Finds lines of species using output from GNRD parser """

    with open(dataPath+'verifiedCanonicalSpeciesNameList'+'_'+fn) as g:
        data = g.readlines()
    data = [x.strip() for x in data]
    content = [x.split("_")[0] for x in data]
    offsetList = [x.split("_")[1] for x in data]

    #offset = int(sys.argv[2])
    newline = 1
    with open(f,'rb') as fd:
    #with open(f,encoding="utf-32") as fd:
    #with open(f,encoding='ISO-8859-1') as fd:
        for species,o in zip(content,offsetList):
            #offsetStart = int(o.split(":")[0])
            #offsetEnd = int(o.split(":")[1])
            #print("offsetStart:",offsetStart)

            fd.seek(offsetStart)
            #line = fd.readline()
            line = fd.read(offsetEnd-offsetStart)
            print("offset Read Line: {} ... SP: {}".format(line,species))

            """
            print("offsetEnd:",offsetEnd)
            fd.seek(offsetEnd)
            #line = fd.readline()
            line = fd.read(1)
            print("offsetEnd Read Line: {} ... SP: {}".format(line,species))
            """


            """
            while True:
                try:
                    byte = fd.read(1)
                    if byte == '\n': newline+=1
                    #print(byte)
                    offset = offset - 1
                    fd.seek(offset)
                except ValueError:
                    break
            """
        print(newline)


def chunks(lst, n):
    """Yield successive n-sized chunks from lst."""
    for i in range(0, len(lst), n):
        yield lst[i:i + n]

def verifySpeciesNamesGNI(fn,f):
    """ Request http://resolver.globalnames.org/name_resolvers.json """

    print("Verifying species names from {}...".format(fn))

    loadName = dataPath+"parsedSpeciesNames"+fn[:-4]+".csv"
    df = pd.read_csv(loadName)

    speciesNameList = df['speciesName']
    l = len(speciesNameList)
    dfVerifiedNames = pd.DataFrame(columns=["speciesName","is_known_name"])
    for shorterList in chunks(speciesNameList,100):
        nameString = "?names="

        for name in shorterList:
            newName = name.replace(" ","+") + "|"
            nameString += newName
        nameString = nameString[:-1]

        best_match_only = "&best_match_only=true" # return one match
        resolve_once = "&resolve_once=true" # first match ==> much faster
        url = "http://resolver.globalnames.org/name_resolvers.json"
        urlFull = url + nameString + best_match_only + resolve_once

        r = requests.get(urlFull)

        for nameDict in r.json()["data"]:
            dfVerifiedNames = dfVerifiedNames.append({
                 "speciesName":nameDict['supplied_name_string'],
                 "is_known_name":nameDict['is_known_name'],
                 }, ignore_index=True)

    saveName = dataPath+"verifiedSpeciesNames"+fn[:-4]+".csv"
    if os.path.exists(saveName):
        os.remove(saveName)
    dfVerifiedNames.to_csv(saveName)

    # remove parsed names df
    if os.path.exists(loadName):
        os.remove(loadName)


def parseSpeciesNamesGNRD(fn,f):
    """ Request http://gnrd.globalnames.org/api """

    print("Parsing species names from {}...".format(fn))

    url = 'http://gnrd.globalnames.org/name_finder.json'
    files = {'file': (fn,open(f, 'rb'))}
    data = {'unique':'1'} # only unique names of species and no offset
    r = requests.post(url, files = files, data = data)

    #df = pd.DataFrame(columns=["speciesName","offsetStart","offsetEnd"])
    df = pd.DataFrame(columns=["speciesName"])
    for nameDict in r.json()["names"]:
        df = df.append({
             "speciesName":nameDict['scientificName'],
             #"offsetStart":nameDict['offsetStart'],
             #"offsetEnd":nameDict['offsetEnd']
             }, ignore_index=True)

    saveName = dataPath+"parsedSpeciesNames"+fn[:-4]+".csv"
    if os.path.exists(saveName):
        os.remove(saveName)
    df.to_csv(saveName)

def read_in_chunks(file_object, chunk_size=1e5):
    """Lazy function (generator) to read a file piece by piece.
    Default chunk size: 10k."""
    while True:
        data = file_object.read(chunk_size)
        if not data:
            break
        yield data



if __name__ == '__main__':
    main()
