import subprocess
import numpy as np
import random
import pandas as pd
import os
import requests
import json
import re
import datetime


def extract(fn,f):
    """ Takes in text file and parses for environment information """

    text_data = []

    currentStation="None"
    currentSounding="None"
    currentLocation="None"
    currentChart="None"
    currentDate="None"
    currentCoords="None"
    currentAirTempNoon="None"
    currentAirTempDailyMean="None"
    currentWaterTempSurface="None"
    currentWaterTempBottom="None"
    currentWaterDensitySurface="None"
    currentWaterDensityBottom="None"
    speciesInfo="None"
    otherLocationInfo="None"

    with open(fn, encoding="utf8") as f:
        for i, l in enumerate(f):
            pass
    lengthOfFile = i + 1


    df = pd.DataFrame()
    with open(fn, encoding="utf8") as f:
        for i,line in enumerate(f):
            print("{}% {}/{}".format(100*round(i/lengthOfFile,2),i,lengthOfFile))

            d={}

            print(line)
            # if sounding in line... get sounding number... then store find the next sounding
            #  Station 16 (Sounding 60)

            if "lat." in line and "long." in line:
                if ", 18" in line or ",18" in line:
                    currentDate = line.split(";")[0]
                    currentCoords = line.split(';')[1]
                else:
                    currentDate = "Not Recorded"
                    currentCoords = line[line.find("lat"):]

            if "Temperature of air" in line:
                # fix for line 33964 of part 1 summary
                # 5.45 p.M. made sail and proceeded towards the Crozet Islands. Temperature of air at
                if ";" in line:
                    currentAirTempNoon = line.split(";")[0]
                    try:
                        currentAirTempDailyMean = line.split(";")[1]
                    except:
                        currentAirTempDailyMean = "Not Recorded"
                else:
                    currentAirTempNoon = line
                    currentAirTempDailyMean = "Not Recorded"


            if "Temperature of water" and ";" in line:
                if ";" in line:
                    currentWaterTempSurface = line.split(";")[0]
                    try:
                        currentWaterTempBottom = line.split(";")[1]
                    except:
                        currentWaterTempBottom = "Not Recorded"
                else:
                    currentWaterTempSurface = line
                    currentWaterTempBottom = "Not Recorded"
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


            if "Density" in line:
                if ";" in line:
                    currentWaterDensitySurface = line.split(";")[0]
                    currentWaterDensityBottom = line.split(";")[1]
                else:
                    currentWaterDensitySurface = line
                    currentWaterDensityBottom = "Not Recorded"
                    # need to fix if in the form:
                    #Density at 60° F. :—

                    #Surface, . . . 1:02739 400 fathoms, . . 102640
                    #100 fathoms, ; 102782 500 - , . 102612
                    #200 , =. . 1:02708 Bottom, . , ; 102607
                    #300 , ~~. , 1:02672


            if "deposit" in line or "Deposit":
                mineralDeposit = line


            d={'currentStation':currentStation,
               'currentSounding':currentSounding,
               'currentLocation':currentLocation,
               'currentChart':currentChart,
               'currentDate':currentDate,
               'currentCoords':currentCoords,
               'currentAirTempNoon':currentAirTempNoon,
               'currentAirTempDailyMean':currentAirTempDailyMean,
               'currentWaterTempSurface':currentWaterTempSurface,
               'currentWaterTempBottom':currentWaterTempBottom,
               'currentWaterDensitySurface':currentWaterDensitySurface,
               'currentWaterTempBottom':currentWaterTempBottom,
               'speciesInfo':speciesInfo,
               'otherLocationInfo':otherLocationInfo}

            print(d)

            df=df.append(d,ignore_index=True)
            print(df.shape)


    df.to_csv(dataPath+"df.csv")
