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
    """ Run info extraction techniques on file """
    fileNameList = ['part1OCR.txt','part2OCR.txt']

    for filename in fileNameList:

        if filename == 'part1OCR.txt':
            continue

        f = dataPath+filename

        stationTextDict = splitTextIntoStations(filename,f)

        dfEnvInfo = getEnvironmentInfo(stationTextDict, filename=filename,f=f)
        #dfEnvInfo = getEnvironmentInfo(filename=filename,f=f)

        dfParsed = parseSpeciesNamesGNRD(stationTextDict,filename=filename,f=f)

        dfVerifiedSpecies = verifySpeciesNamesGNI(dfParsed, filename=filename, f=f)
        #dfVerifiedSpecies = verifySpeciesNamesGNI(filename=filename, f=f)

    # merge files from part 1 and part 2 summaries
    mergefiles(fileNameList)




def mergefiles(fileNameList):

    filenamePart1 = fileNameList[0]
    filenamePart2 = fileNameList[1]

    fPart1Summary = dataPath+fileNameList[0]
    fPart2Summary = dataPath+fileNameList[1]

    part1dfStationLines = dataPath+"dfStationLines_"+filenamePart1[:-4]+".csv"
    part2dfStationLines = dataPath+"dfStationLines_"+filenamePart2[:-4]+".csv"
    print("part1dfStationLines",part1dfStationLines)
    print("part2dfStationLines",part2dfStationLines)

    part1verifiedSpeciesNames = dataPath+"verifiedSpeciesNames_"+filenamePart1[:-4]+".csv"
    part2verifiedSpeciesNames = dataPath+"verifiedSpeciesNames_"+filenamePart2[:-4]+".csv"
    print("part1verifiedSpeciesNames",part1verifiedSpeciesNames)
    print("part2verifiedSpeciesNames",part2verifiedSpeciesNames)

    part1stationEnvironmentInfo = dataPath+"dfStationEnvironmentInfo_"+filenamePart1[:-4]+".csv"
    part2stationEnvironmentInfo = dataPath+"dfStationEnvironmentInfo_"+filenamePart2[:-4]+".csv"
    print("part1stationEnvironmentInfo",part1stationEnvironmentInfo)
    print("part2stationEnvironmentInfo",part2stationEnvironmentInfo)

    try:
        df_part1StationLines = pd.read_csv(part1dfStationLines)
        df_part2StationLines = pd.read_csv(part2dfStationLines)
        df_StationLines = pd.concat([df_part1StationLines,df_part2StationLines]).reset_index()
        saveName_df_stationLines = dataPath+"stationLines_allStations.csv"
        df_StationLines.to_csv(saveName_df_stationLines)
    except:
        pass

    try:
        df_part1verifiedSpeciesNames = pd.read_csv(part1verifiedSpeciesNames)
        df_part2verifiedSpeciesNames = pd.read_csv(part2verifiedSpeciesNames)
        df_verifiedSpeciesNames = pd.concat([df_part1verifiedSpeciesNames,df_part2verifiedSpeciesNames]).reset_index()
        saveName_df_verifiedSpeciesNames = dataPath+"verifiedSpeciesNames_allStations.csv"
        df_verifiedSpeciesNames.to_csv(saveName_df_verifiedSpeciesNames)
    except:
        pass

    try:
        df_part1stationEnvironmentInfo = pd.read_csv(part1stationEnvironmentInfo)
        df_part2stationEnvironmentInfo = pd.read_csv(part2stationEnvironmentInfo)
        df_stationEnvironmentInfo = pd.concat([df_part1stationEnvironmentInfo,df_part2stationEnvironmentInfo]).reset_index()
        saveName_df_stationEnvironmentInfo = dataPath+"stationEnvironmentInfo_allStations.csv"
        df_stationEnvironmentInfo.to_csv(saveName_df_stationEnvironmentInfo)
    except:
        pass



def splitTextIntoStations(filename,f):
    """ Break summary texts into sections for each station, put in dictionary"""

    print("Splitting {} into stations...".format(filename))

    with open(f,'r') as fd:
        for i, l in enumerate(fd):
            pass
    lengthOfFile = i

    fString = io.open(f, mode="r", encoding="utf-8")
    text = fString.read()

    dfStationLines = pd.DataFrame(columns=["station","startLine"])
    stationTextDict = {}
    processingFirstStation = True
    with open(f,'r') as f:
        for i,line in enumerate(f):

            #if i == 0:
            #    prevStationLine = line


            print("{}% Line {}/{}".format(100*round(i/lengthOfFile,2),i,lengthOfFile))
            print(line)

            # example: Station 16 (Sounding 60)
            # for first station through station before last station
            if "(Sounding" in line:

                if processingFirstStation:
                    prevStation = line.split("(Sounding")[0]
                    prevStationLine = line
                    previ = i
                    processingFirstStation = False
                    continue




                dfStationLines = dfStationLines.append({'station':prevStation,
                                                        'startLine':previ},
                                                        ignore_index=True)

                station = line.split("(Sounding")[0]

                print("Found {}!".format(station))

                # text before new station
                textBefore = text.split(line)[0]

                # text after prev station and before new station
                # so this is the text for the prev station
                textInPrevStation = textBefore.split(prevStationLine)[1]

                # store text for each station in a dictionary

                stationTextDict[prevStation] = textInPrevStation

                prevStation = station
                prevStationLine = line
                previ = i

            # for last station
            if i == lengthOfFile:
                print("lastLineWithText",lastLineWithText)
                print("line",line)

                dfStationLines = dfStationLines.append({'station':prevStation,
                                                        'startLine':previ},
                                                        ignore_index=True)

                # text before new station
                textBefore = text.split(lastLineWithText)[0]

                # text after prev station and before new station
                # so this is the text for the prev station
                textInPrevStation = textBefore.split(prevStationLine)[1]

                # store text for each station in a dictionary

                stationTextDict[prevStation] = textInPrevStation


            if line != "\n" and line != "\t" and line != "" and line != " "\
            and re.sub("[^0-9]", "", line) != "":
                lastLineWithText = line

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

    saveName = dataPath+"dfStationLines_"+filename[:-4]+".csv"
    if os.path.exists(saveName):
        os.remove(saveName)
    dfStationLines.to_csv(saveName)

    saveName = dataPath+"stationTextDict_"+filename[:-4]+".pkl"
    if os.path.exists(saveName):
        os.remove(saveName)
    with open(saveName, 'wb') as f:
        pickle.dump(stationTextDict, f)

    return stationTextDict



def parseSpeciesNamesGNRD(stationTextDict, filename, f):

    """ Request http://gnrd.globalnames.org/api """

    print("Parsing species names from {}...".format(filename))

    loadname = dataPath+"stationTextDict_"+filename[:-4]+".pkl"
    with open(loadname, 'rb') as f:
        stationTextDict = pickle.load(f)

    columnsList = ["station","speciesName","offsetStart","offsetEnd"]
    df = pd.DataFrame(columns=columnsList)

    for key,val in stationTextDict.items():
        url = 'http://gnrd.globalnames.org/name_finder.json'

        #text_file = open(dataPath+"tempFile.txt", "wb")
        #text_file.write(val)
        #text_file.close()
        #fileFromString = open(dataPathPath+"tempFile.txt","rb")

        fileFromString = io.StringIO(val)
        print("type(fileFromString)",type(fileFromString))
        print(val[0:10])
        print(key)

        files = {'file': (filename, fileFromString)}
        #data = {'unique':'1'} # only unique names of species and no offset
        #r = requests.post(url, files = files, data = data)
        r = requests.post(url, files = files) # get names with offsetStart, offsetEnd
        print(r)

        try:
            print(r.json())

            #df = pd.DataFrame(columns=["speciesName","offsetStart","offsetEnd"])
            for nameDict in r.json()["names"]:
                df = df.append({
                     "station":key,
                     "speciesName":nameDict['scientificName'],
                     "offsetStart":nameDict['offsetStart'],
                     "offsetEnd":nameDict['offsetEnd']
                     }, ignore_index=True)
        except:
            print("No .json was returned by the server - skipping station!")
            pass

    saveName = dataPath+"parsedSpeciesNamesWithStationAndOffset_"+filename[:-4]+".csv"
    if os.path.exists(saveName):
        os.remove(saveName)
    df.to_csv(saveName)

    return df

def verifySpeciesNamesGNI(df, filename, f):
#def verifySpeciesNamesGNI(filename, f):

    """ Request http://resolver.globalnames.org/name_resolvers.json """

    print("Verifying species names from {}...".format(filename))

    loadName = dataPath+"parsedSpeciesNamesWithStationAndOffset_"+filename[:-4]+".csv"

    df = pd.read_csv(loadName)

    isKnownNameList = []
    dataSourceList = []
    gniUUIDList = []
    classificationPathList = []
    classificationPathRankList = []
    vernacularsList = []
    canonicalFormList = []

    for i,row in df.iterrows():
        nameString = "?names="
        name = row.speciesName
        print("Checking {}...".format(name))
        nameString = "names="+name.replace(" ","+")
        #newName = name.replace(" ","+") + "|"
        #nameString += newName
        #nameString = nameString[:-1]

        # parameters
        # {'with_context': False,
        # 'header_only': False,
        # 'with_canonical_ranks': False,
        # 'with_vernaculars': False,
        # 'best_match_only': True,
        # 'data_sources': [],
        # 'preferred_data_sources': [],
        # 'resolve_once': True}}
        with_context = "&with_context=true"
        header_only = "&header_only=false"
        with_canonical_ranks = "&with_canonical_ranks=true"
        with_vernaculars = "&with_vernaculars=true"
        best_match_only = "&best_match_only=true"
        resolve_once = "&resolve_once=false"
        #resolve_once = "&resolve_once=true" # first match ==> much faster
        urlGNI = "http://resolver.globalnames.org/name_resolvers.json?"
        urlFull = urlGNI + nameString + with_context \
                                      + header_only \
                                      + with_canonical_ranks \
                                      + with_vernaculars \
                                      + best_match_only \
                                      + resolve_once
        #urlFull = urlGNI + nameString + best_match_only + resolve_once
        # the stuff that is commented out now was the query for quick runs

        print(urlFull)

        try:
            r = requests.get(urlFull)
            print(r)

            # r.json() is what is returned by the server
            print(r.json())
            print("Verified {}.".format(name))
            for nameDict in r.json()["data"]:

                """
                print("nameDict",nameDict)
                print("nameDict['results']",nameDict['results'])
                print("type(nameDict['results'])",type(nameDict['results']))
                print("nameDict['results'][0]",nameDict['results'][0])
                print("type(nameDict['results'][0])",type(nameDict['results'][0]))
                """

                try:
                    isKnownNameList.append(nameDict['is_known_name'])
                    print("nameDict['is_known_name']",
                           nameDict['is_known_name'])
                except Exception as e:
                    print(e)
                    isKnownNameList.append("")

                try:
                    dataSourceList.append(nameDict['results'][0]['data_source_title'])
                    print("nameDict['results'][0]['data_source_title']",
                           nameDict['results'][0]['data_source_title'])
                except Exception as e:
                    print(e)
                    dataSourceList.append("")

                try:
                    gniUUIDList.append(nameDict['results'][0]['gni_uuid'])
                    print("nameDict['results'][0]['gni_uuid']",
                           nameDict['results'][0]['gni_uuid'])
                except Exception as e:
                    print(e)
                    gniUUIDList.append("")

                try:
                    canonicalFormList.append(nameDict['results'][0]['canonical_form'])
                    print("nameDict['results'][0]['canonical_form']",
                           nameDict['results'][0]['canonical_form'])
                except Exception as e:
                    print(e)
                    canonicalFormList.append("")

                try:
                    classificationPathList.append(nameDict['results'][0]['classification_path'])
                    print("nameDict['results'][0]['classification_path']",
                           nameDict['results'][0]['classification_path'])
                except Exception as e:
                    print(e)
                    classificationPathList.append("")

                try:
                    classificationPathRankList.append(nameDict['results'][0]['classification_path_ranks'])
                    print("nameDict['results'][0]['classification_path_ranks']",
                           nameDict['results'][0]['classification_path_ranks'])
                except Exception as e:
                    print(e)
                    classificationPathRankList.append("")

                try:
                    vernacularsList.append(nameDict['results'][0]['vernaculars'][0]['name'])
                    print("nameDict['results'][0]['vernaculars'][0]['name']",
                           nameDict['results'][0]['vernaculars'][0]['name'])
                except Exception as e:
                    print(e)
                    vernacularsList.append("")


                #dfVerifiedNames = dfVerifiedNames.append({
                #     "speciesName":nameDict['supplied_name_string'],
                #     "is_known_name":nameDict['is_known_name'],
                #     }, ignore_index=True)
        except Exception as e:
            print(e)
            isKnownNameList.append("")
            dataSourceList.append("")
            gniUUIDList.append("")
            canonicalFormList.append("")
            classificationPathList.append("")
            classificationPathRankList.append("")
            vernacularsList.append("")


    df['vernacular'] = vernacularsList
    df['canonicalForm'] = canonicalFormList
    df['verified'] = isKnownNameList
    df['dataSource'] = dataSourceList
    df['gniUUID'] = gniUUIDList
    df['classificationPath'] = classificationPathList
    df['classificationPathRank'] = classificationPathRankList



    # old version
    """
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
    """

    saveName = dataPath+"verifiedSpeciesNames_"+filename[:-4]+".csv"
    if os.path.exists(saveName):
        os.remove(saveName)
    df.to_csv(saveName)
    #dfVerifiedNames.to_csv(saveName)

    # remove parsed names df
    #if os.path.exists(loadName):
    #    os.remove(loadName)


def getEnvironmentInfo(stationTextDict, filename,f):
#def getEnvironmentInfo(filename,f):

    """ Takes in text file and parses for environment information """

    loadname = dataPath+"stationTextDict_"+filename[:-4]+".pkl"
    with open(loadname, 'rb') as file:
        stationTextDict = pickle.load(file)

    text_data = []

    columnNames = ['currentStation',
                   'currentDate',
                   'currentDMSCoords',
                   'currentLatDegree',
                   'currentLatMinute',
                   'currentLatSecond',
                   'currentLatCoord',
                   'currentLongDegree',
                   'currentLongMinute',
                   'currentLongSecond',
                   'currentLongCoord',
                   'currentAirTempNoon',
                   'currentAirTempNoonDegree',
                   'currentAirTempDailyMean',
                   'currentAirTempDailyMeanDegree',
                   'currentWaterTempSurface',
                   'currentWaterTempSurfaceDegree',
                   'currentWaterTempBottom',
                   'currentWaterTempBottomDegree',
                   'currentWaterDensitySurface',
                   'currentWaterDensitySurfaceNumber',
                   'currentWaterDensityBottom',
                   'currentWaterDensityBottomNumber',
                   'lineNumberOfDate',
                   'lineNumberOfLatLong',
                   'lineNumberAirTempNoon',
                   'lineNumberOfAirTempDailyMean',
                   'lineNumberOfWaterTempSurface',
                   'lineNumberOfWaterTempBottom',
                   'lineNumberOfWaterDensitySurface',
                   'lineNumberOfWaterDensityBottom'
                   ]

    df = pd.DataFrame(columns = columnNames)
    stationsSeenSoFar = 0

    lineNumberFromBeginningOfText = 0

    for key,val in stationTextDict.items():

        currentStation=key
        currentDate=""
        currentDMSCoords=""
        currentLatDegree = ""
        currentLatMinute = ""
        currentLatSecond = ""
        currentLatCoord = ""
        currentLongDegree = ""
        currentLongMinute = ""
        currentLongSecond = ""
        currentLongCoord = ""
        currentAirTempNoon=""
        currentAirTempNoonDegree=""
        currentAirTempDailyMean=""
        currentAirTempDailyMeanDegree=""
        currentWaterTempSurface=""
        currentWaterTempSurfaceDegree=""
        currentWaterTempBottom=""
        currentWaterTempBottomDegree=""
        currentWaterDensitySurface=""
        currentWaterDensitySurfaceNumber=""
        currentWaterDensityBottom=""
        currentWaterDensityBottomNumber=""
        lineNumberOfDate=""
        lineNumberOfLatLong=""
        lineNumberAirTempNoon=""
        lineNumberOfAirTempDailyMean=""
        lineNumberOfWaterTempSurface=""
        lineNumberOfWaterTempBottom=""
        lineNumberOfWaterDensitySurface=""
        lineNumberOfWaterDensityBottom=""

        stationsSeenSoFar += 1

        print("type val",type(val))

        fileFromString = io.StringIO(val)
        print("type fileFromString",type(fileFromString))
        #print("type(fileFromString)",type(fileFromString))
        #print(val)
        #print(key)

        for i, l in enumerate(fileFromString):
            continue
        lenText = i + 1
        print("Station {} text has {} lines".format(key,lenText))


        #lineResult = libLAPFF.parseLine(line)


        #for j,l in enumerate(fileFromString):

        #line = fileFromString.readline()

        lineNumberFromStationBegin = 0

        firstLatLong = True
        firstAirTemp = True
        firstWaterTemp = True
        firstDensity = True
        #firstDeposit = True
        for line in val.splitlines():

            lineNumberFromStationBegin += 1

            lineNumberFromBeginningOfText += 1
            #print(line)

            #print("{}% {}/{}".format(100*round(j/lenText,2),j,lenText))

            d={}

            #line = fileFromString.readline()
            #print(fileFromString)

            #print(line)
            #print("lineNumber",lineNumber)

            # if sounding in line... get sounding number... then store find the next sounding
            #  Station 16 (Sounding 60)

            # need to convert degrees minutes seconds (DMS) Latitude longtitude
            # into standard latitude longtitude coordiantes
            if "lat." in line and "long." in line and firstLatLong:
                #print("lat and long",line)
                #time.sleep(3)
                if ", 18" in line or ",18" in line: # 18 for 1876, 1877...
                    try:
                        currentDate = line.split(";")[0]
                        currentDMSCoords = line.split(';')[1]


                        lineNumberOfDate=lineNumberFromBeginningOfText
                        lineNumberOfLatLong=lineNumberFromBeginningOfText

                        print("line",line)
                        print("currentDate",currentDate)
                        print("currentDMSCoords",currentDMSCoords)



                        firstLatLong = False

                        try:
                            currentLatDegree = currentDMSCoords.split(",")[0].split("°")[0]
                            currentLatDegree = re.sub("[^0-9]", "", currentLatDegree)

                        except Exception as e:
                            print(e)
                            currentLatDegree = "{}, {}".format(e,line)


                        if "’" in currentDMSCoords.split(",")[0].split("°")[1]:
                            try:
                                currentLatMinute = currentDMSCoords.split(",")[0].split("°")[1].split("’")[0]
                            except Exception as e:
                                print(e)
                                currentLatMinute = ""
                            try: # none of these so far, but just in case
                                currentLatSecond = currentDMSCoords.split(",")[0].split("°")[1].split("’")[1]
                                currentLatSecond =  re.sub("[^0-9]", "", currentLatSecond)
                            except Exception as e:
                                print(e)
                                currentLatSecond = ""
                        if "'" in currentDMSCoords.split(",")[0].split("°")[1]:
                            try:
                                currentLatMinute = currentDMSCoords.split(",")[0].split("°")[1].split("'")[0]
                            except Exception as e:
                                print(e)
                                currentLatMinute = ""
                            try: # none of these so far, but just in case
                                currentLatSecond = currentDMSCoords.split(",")[0].split("°")[1].split("'")[1]
                                currentLatSecond =  re.sub("[^0-9]", "", currentLatSecond)
                            except Exception as e:
                                print(e)
                                currentLatSecond = ""

                        print("currentLatDegree",currentLatDegree)
                        print("currentLatMinute",currentLatMinute)
                        print("currentLatSecond",currentLatSecond)


                        if currentLatSecond == "" and currentLatMinute == "":
                            currentLatCoord = float(currentLatDegree)
                        elif currentLatSecond == "" and currentLatMinute != "":
                            currentLatCoord = float(currentLatDegree) \
                                            + float(currentLatMinute) / 60
                        else:
                            currentLatCoord = float(currentLatDegree) \
                                            + float(currentLatMinute) / 60 \
                                            + float(currentLatSecond) / 3600

                        print("currentLatCoord",currentLatCoord)

                        try:
                            currentLongDegree = currentDMSCoords.split(",")[1].split("°")[0]
                            currentLongDegree = re.sub("[^0-9]", "", currentLongDegree)
                        except Exception as e:
                            print(e)
                            currentLongDegree = "{}, {}".format(e,line)

                        if "’" in currentDMSCoords.split(",")[1].split("°")[1]:
                            try:
                                currentLongMinute = currentDMSCoords.split(",")[1].split("°")[1].split("’")[0]
                            except Exception as e:
                                print(e)
                                currentLongMinute = ""
                            try: # none of these so far, but just in case
                                currentLongSecond = currentDMSCoords.split(",")[1].split("°")[1].split("’")[1]
                                currentLongSecond =  re.sub("[^0-9]", "", currentLongSecond)
                            except Exception as e:
                                print(e)
                                currentLongSecond = ""

                        if "'" in currentDMSCoords.split(",")[1].split("°")[1]:
                            try:
                                currentLongMinute = currentDMSCoords.split(",")[1].split("°")[1].split("'")[0]
                            except Exception as e:
                                print(e)
                                currentLongMinute = ""
                            try: # none of these so far, but just in case
                                currentLongSecond = currentDMSCoords.split(",")[1].split("°")[1].split("'")[1]
                                currentLongSecond =  re.sub("[^0-9]", "", currentLongSecond)
                            except Exception as e:
                                print(e)
                                currentLongSecond = ""

                        print("currentLongDegree",currentLongDegree)
                        print("currentLongMinute",currentLongMinute)
                        print("currentLongSecond",currentLongSecond)

                        if currentLongSecond == "" and currentLongMinute == "":
                            currentLongCoord = float(currentLongDegree)
                        elif currentLongSecond == "" and currentLongMinute != "":
                            currentLongCoord = float(currentLongDegree) \
                                             + float(currentLongMinute) / 60
                        else:
                            currentLongCoord = float(currentLongDegree) \
                                             + float(currentLongMinute) / 60 \
                                             + float(currentLongSecond) / 3600

                        print("currentLongCoord",currentLongCoord)


                    except Exception as e:
                        print(e)
                        currentDate = "{}, {}".format(e,line)
                        currentDMSCoords = "{}, {}".format(e,line)

                    #time.sleep(3)

                else:

                    currentDate = ""
                    currentDMSCoords = line[line.find("lat"):]



            if "Temperature of air" in line and firstAirTemp:
                firstAirTemp = False
                #print("temp air",line)
                #time.sleep(3)

                lineNumberAirTempNoon=lineNumberFromBeginningOfText
                lineNumberOfAirTempDailyMean=lineNumberFromBeginningOfText

                # fix for line 33964 of part 1 summary
                # 5.45 p.M. made sail and proceeded towards the Crozet Islands. Temperature of air at
                if ";" in line:
                    currentAirTempNoon = line.split(";")[0]
                    currentAirTempNoonDegree = currentAirTempNoon.split(",")[1]
                    currentAirTempNoonDegree = re.sub("[^0-9]", "", \
                                                currentAirTempNoonDegree)
                    currentAirTempNoonDegree = currentAirTempNoonDegree[0:-1]+\
                                                "."+currentAirTempNoonDegree[-1]

                    try:
                        currentAirTempDailyMean = line.split(";")[1]
                        currentAirTempDailyMeanDegree = currentAirTempDailyMean.split(",")[1]
                        try:
                            currentAirTempDailyMeanDegree = currentAirTempDailyMeanDegree.split(".")[0]
                            currentAirTempDailyMeanDegree = re.sub("[^0-9]", "", \
                                                            currentAirTempDailyMeanDegree)
                            currentAirTempDailyMeanDegree = currentAirTempDailyMeanDegree[0:-1]+\
                                                            "."+currentAirTempDailyMeanDegree[-1]

                        except:
                            currentAirTempDailyMeanDegree = re.sub("[^0-9]", "", \
                                                            currentAirTempDailyMeanDegree)
                            currentAirTempDailyMeanDegree = currentAirTempDailyMeanDegree[0:-1]+\
                                                            "."+currentAirTempDailyMeanDegree[-1]

                    except:
                        currentAirTempDailyMean = ""
                else:
                    currentAirTempNoon = line
                    currentAirTempNoonDegree = currentAirTempNoon.split(",")[1]
                    currentAirTempNoonDegree = re.sub("[^0-9]", "", \
                                                currentAirTempNoonDegree)
                    currentAirTempNoonDegree = currentAirTempNoonDegree[0:-1]+\
                                                "."+currentAirTempNoonDegree[-1]
                    currentAirTempDailyMean = ""
                    currentAirTempDailyMeanDegree = ""

            if "Temperature of water" in line and firstWaterTemp:
                firstWaterTemp = False
                #print("temp water",line)
                #time.sleep(3)

                lineNumberOfWaterTempSurface=lineNumberFromBeginningOfText
                lineNumberOfWaterTempBottom=lineNumberFromBeginningOfText

                if ";" in line:
                    currentWaterTempSurface = line.split(";")[0]
                    try:
                        currentWaterTempSurfaceDegree = currentWaterTempSurface.split(",")[1]
                        currentWaterTempSurfaceDegree = re.sub("[^0-9]", "", \
                                                        currentWaterTempSurfaceDegree)
                        currentWaterTempSurfaceDegree = currentWaterTempSurfaceDegree[0:-1]+\
                                                        "."+currentWaterTempSurfaceDegree[-1]

                    except:
                        #currentWaterTempSurfaceDegree = ""
                        currentWaterTempSurfaceDegree = re.sub("[^0-9]", "", \
                                                        currentWaterTempSurface)
                        currentWaterTempSurfaceDegree = currentWaterTempSurfaceDegree[0:-1]+\
                                                        "."+currentWaterTempSurfaceDegree[-1]

                    try:
                        currentWaterTempBottom = line.split(";")[1]
                        currentWaterTempBottomDegree = currentWaterTempBottom.split(",")[1]
                        try:
                            currentWaterTempBottomDegree = currentWaterTempBottomDegree.split(".")[0]
                            currentWaterTempBottomDegree = re.sub("[^0-9]", "", \
                                                            currentWaterTempBottomDegree)
                            currentWaterTempBottomDegree = currentWaterTempBottomDegree[0:-1]+\
                                                            "."+currentWaterTempBottomDegree[-1]

                        except:
                            currentWaterTempBottomDegree = currentWaterTempBottomDegree
                            currentWaterTempBottomDegree = re.sub("[^0-9]", "", \
                                                            currentWaterTempBottomDegree)
                            currentWaterTempBottomDegree = currentWaterTempSurfaceDegree[0:-1]+\
                                                            "."+currentWaterTempBottomDegree[-1]

                    except:
                        currentWaterTempBottom = ""
                else:
                    currentWaterTempSurface = line
                    try:
                        currentWaterTempSurfaceDegree = currentWaterTempSurface.split(",")[1]
                        currentWaterTempSurfaceDegree = re.sub("[^0-9]", "", \
                                                        currentWaterTempSurfaceDegree)
                        currentWaterTempSurfaceDegree = currentWaterTempSurfaceDegree[0:-1]+\
                                                        "."+currentWaterTempSurfaceDegree[-1]
                    except:
                        currentWaterTempSurfaceDegree = ""

                    currentWaterTempBottom = ""
                    currentWaterTempBottomDegree = ""
                    # need to fix if in the form:
                    #Temperature of water :—
                    #
                    #
                    #
                    #Surface, . . . . 72'5 900 fathoms, . . . 39°8
                    #100 fathoms, . , . 66:5 1000 _—sé=éy«y . . . 39°3
                    #200_ Cs, . , ; 60°3 1100 _s=»“"»~ . . . 38°8
                    #300_—SC=é»; , , . 53°8 1200 __s=»“", . . . 38°3
                    #400_ ,, , . ~ 475 1300 _—sé=é“»"» . . . 37°9
                    #500 _—Ssé=»; ; . . 43°2 1400 _ _,, . . . 37°5
                    #600 _,, , , . 41°6 1500 _—=s=é»; : , . 71
                    #700_—=C=»y . . , 40°7 Bottom, . ; . , 36°2
                    #800 __—,, . . , 40°2


            if "Density" in line and firstDensity:
                firstDensity = False

                #print("density",line)
                #time.sleep(3)

                lineNumberOfWaterDensitySurface=lineNumberFromBeginningOfText
                lineNumberOfWaterDensityBottom=lineNumberFromBeginningOfText

                if ";" in line:
                    currentWaterDensitySurface = line.split(";")[0]
                    currentWaterDensitySurfaceNumber = currentWaterDensitySurface.split(",")[1]
                    currentWaterDensitySurfaceNumber = re.sub("[^0-9]", "", \
                                                    currentWaterDensitySurfaceNumber)
                    currentWaterDensitySurfaceNumber = currentWaterDensitySurfaceNumber[0]+\
                                                    "."+currentWaterDensitySurfaceNumber[1:]

                    currentWaterDensityBottom = line.split(";")[1]
                    currentWaterDensityBottomNumber = currentWaterDensityBottom.split(",")[1]
                    try:
                        currentWaterDensityBottomNumber = currentWaterDensityBottomNumber.split(".")[0]
                        currentWaterDensityBottomNumber = re.sub("[^0-9]", "", \
                                                        currentWaterDensityBottomNumber)
                        currentWaterDensityBottomNumber = currentWaterDensityBottomNumber[0]+\
                                                        "."+currentWaterDensityBottomNumber[1:]

                    except:
                        currentWaterDensityBottomNumber = re.sub("[^0-9]", "", \
                                                        currentWaterDensityBottomNumber)
                        currentWaterDensityBottomNumber = currentWaterDensityBottomNumber[0]+\
                                                        "."+currentWaterDensityBottomNumber[1:]

                else:
                    try:
                        currentWaterDensitySurface = line
                        currentWaterDensitySurfaceNumber = currentWaterDensitySurface.split(",")[1]
                        currentWaterDensitySurfaceNumber = re.sub("[^0-9]", "", \
                                                        currentWaterDensitySurfaceNumber)
                        currentWaterDensitySurfaceNumber = currentWaterDensitySurfaceNumber[0]+\
                                                        "."+currentWaterDensitySurfaceNumber[1:]


                    except:
                        currentWaterDensitySurface = ""
                        currentWaterDensitySurfaceNumber = ""

                    currentWaterDensityBottom = ""
                    currentWaterDensityBottomNumber = ""
                    # need to fix if in the form:
                    #Density at 60° F. :—

                    #Surface, . . . 1:02739 400 fathoms, . . 102640
                    #100 fathoms, ; 102782 500 - , . 102612
                    #200 , =. . 1:02708 Bottom, . , ; 102607
                    #300 , ~~. , 1:02672

            """
            if "deposit " in line or "Deposit " in line and firstDeposit:
                firstDeposit = False
                print("deposit",line)
                time.sleep(3)

                mineralDeposit = line
            """

            d={'currentStation':currentStation,
               'currentDate':currentDate,
               'currentDMSCoords':currentDMSCoords,
               'currentLatDegree':currentLatDegree,
               'currentLatMinute':currentLatMinute,
               'currentLatSecond':currentLatSecond,
               'currentLatCoord':currentLatCoord,
               'currentLongDegree':currentLongDegree,
               'currentLongMinute':currentLongMinute,
               'currentLongSecond':currentLongSecond,
               'currentLongCoord':currentLongCoord,
               'currentAirTempNoon':currentAirTempNoon,
               'currentAirTempNoonDegree':currentAirTempNoonDegree,
               'currentAirTempDailyMean':currentAirTempDailyMean,
               'currentAirTempDailyMeanDegree':currentAirTempDailyMeanDegree,
               'currentWaterTempSurface':currentWaterTempSurface,
               'currentWaterTempSurfaceDegree':currentWaterTempSurfaceDegree,
               'currentWaterTempBottom':currentWaterTempBottom,
               'currentWaterTempBottomDegree':currentWaterTempBottomDegree,
               'currentWaterDensitySurface':currentWaterDensitySurface,
               'currentWaterDensitySurfaceNumber':currentWaterDensitySurfaceNumber,
               'currentWaterDensityBottom':currentWaterDensityBottom,
               'currentWaterDensityBottomNumber':currentWaterDensityBottomNumber,
               'lineNumberOfDate':lineNumberOfDate,
               'lineNumberOfLatLong':lineNumberOfLatLong,
               'lineNumberAirTempNoon':lineNumberAirTempNoon,
               'lineNumberOfAirTempDailyMean':lineNumberOfAirTempDailyMean,
               'lineNumberOfWaterTempSurface':lineNumberOfWaterTempSurface,
               'lineNumberOfWaterTempBottom':lineNumberOfWaterTempBottom,
               'lineNumberOfWaterDensitySurface':lineNumberOfWaterDensitySurface,
               'lineNumberOfWaterDensityBottom':lineNumberOfWaterDensityBottom
               }


            #print("lineNumber",lineNumber)
            #print("lenText",lenText)

            if lineNumberFromStationBegin == lenText:


                print(d)

                df = df.append(d,ignore_index=True)

                print("stationsSeenSoFar:",stationsSeenSoFar)

                print(df.shape)




    df.to_csv(dataPath+"dfStationEnvironmentInfo_"+filename[:-4]+".csv",encoding='utf-8-sig')


if __name__ == '__main__':
    main()
