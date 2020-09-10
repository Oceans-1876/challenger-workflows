# oceans-1876-data-pipeline

WARNING: This is an old version and inefficient version with 2 main downsides that I am fixing.
1. It calls the online service for species recognition through the API. 
2. This version has to use the functions in getSpeciesInfoNoOffset.py which parse through the text again to find the line numbers of the recognized species names returned by the online service. A much better idea is to use getSpeciesInfoWithOffset.py which relies on the offsetStart and offsetEnd returned from the species-recognition online service to locate the species' line numbers in the text.

Setup: text files in /data/ folder. Also, set up rootPath and dataPath in main.py 
Usage: python main.py

Explanation: 
- main.py calls getEnvironmentInfo.py, getSpeciesInfo.py, and cleanup.py as modules.

- getEnvironmentInfo.extract() parses through the files to get environment information (air, water, depth, etc), then associates that info with each station.

- getSpeciesInfo.extract() calls getSpeciesInfoNoOffset.py, which calls the species recognition online service through the API, then associates info with each stations.

- cleanup.cleanupDataDirectory() deletes the files not used anymore.
