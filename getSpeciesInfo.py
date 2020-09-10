#!/usr/bin/env python3
import getSpeciesInfoNoOffset
import getSpeciesInfoWithOffset

def extract(fn,f):
    """ Get information about species from file by calling API """

    # call API to recognize names, then do parse through the script again
    # to refind the species names line locations. Inefficient but only method
    # that works until we can figure out how to use offsetStart & offsetEnd
    getSpeciesInfoNoOffset.getSpeciesNames(fn=fn,f=f)
    getSpeciesInfoNoOffset.getStationOfSpecies(fn=fn,f=f)

    ### WARNING: don't use until fixing script that uses offsetStart & offsetEnd
    # getSpeciesInfoWithOffset.getSpeciesInfo(fn=fn,f=f)
    # getSpeciesInfoWithOffset.getStationOfSpecies(fn=fn,f=f)
