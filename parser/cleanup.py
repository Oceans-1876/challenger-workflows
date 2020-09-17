import os

def cleanupDataDirectory(fn,f):
    """ Delete .csv files that we don't need anymore """

    print("TODO")

    filesToRemoveList = []
    for file in filesToRemoveList:
        if os.path.exists(file):
            try:
                os.remove(file)
            except:
                print("File {} does not exist".format(file))
                pass
