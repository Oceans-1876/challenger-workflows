#!/usr/bin/env python3
import getSpeciesInfo
import getEnvironmentInfo
import cleanup

rootPath = "/home/mwiecksosa/oceans1876/"
dataPath = rootPath+"data/"

def main():
    """ Run info extraction techniques on file """
    #fileNameList = ['part1OCR.txt','part2OCR.txt']
    fileNameList = ['part1OCR.txt'] # test on one file first
    for fn in fileNameList:
        f = dataPath+fn
        getEnvironmentInfo.extract(fn=fn,f=f)
        getSpeciesInfo.extract(fn=fn,f=f)
        cleanup.cleanupDataDirectory(fn=fn,f=f)


if __name__ == '__main__':
    main()
