#
# filename: output_fileset_directory
#
#***********************************************
# Python APIs demonstrated:
# class
# __init__
# call instance methods self.f1()
# raise exceptions
# private methods
# public methods
# path.exists
# path.isdir
#
# Functionality:
# ***************************
# 1. Given a root directory and a filsetDirectory
# 2. make sure ONLY on first use
# 3. the directory is empty 
# 4. Delete files if necessary
#
# usage
# ***************************
# fsdir = FileSetDirectory(root, someDataDirName)
# fsdirname = fsdir.getDirectory()
# print(fsdirname) #is a concatenated string
#
#***********************************************
#
#Import for working with files and directories
from os import path
import os
import shutil

#import datetime obect from datetime standard library
from datetime import datetime

class FileSetDirectory:

    def __init__(self, rootDirectory, fileSetDirectoryName):
        self.rootDirectory = rootDirectory
        self.fileSetDirectoryName = fileSetDirectoryName
        self.firstUseFlag = True
        self.validatedFlag = False
        self.__validateRootDirectory__()

    #if it is not an existing directory
    #Raise an error
    def __validateRootDirectory__(self):
        if path.isdir(self.rootDirectory) == False:
            #This is not a path
            raise RuntimeError("{} as a directory does nt exist"
                    .format(self.rootDirectory))

    #*****************************************
    # _emptyFilesetDirectory
    #*****************************************
    #Call this method 
    def validateOnFirstUse(self):
        #does it exist
        #if it exists delete its files
        #if not create the directory
        #Note: Does not touch the firstUseFlag

        print("validateOnFirstUse called. This should be only once")

        rootDirectory = self.rootDirectory
        fileSetDirectory = self.fileSetDirectoryName

        #if root directory is not a directory
        #raise an error
        if path.isdir(rootDirectory) == False: 
            raise RuntimeError("{} is not a directory"
                .format(rootDirectory))

        #it is a directory. so there is a root
        fullFilesetDirectory = self.internalGetDirectoryName()

        #if it exists clear files and return
        if path.exists(fullFilesetDirectory):
            #clear files
            self._emptyFilesetDirectory(fullFilesetDirectory)
        else:
            #this directory doesn't exist
            print("Creating directory:", fullFilesetDirectory)
            os.makedirs(fullFilesetDirectory)

        return True

    #*****************************************
    # _emptyFilesetDirectory
    #*****************************************
    def _emptyFilesetDirectory(self, fileSetDirectory):
        print ("Clearing up directory:", fileSetDirectory)
        fileList = os.listdir(fileSetDirectory)
        numOfFiles = len(fileList)

        print ("Number of files in the directory:", numOfFiles)
        if (numOfFiles == 0):
            print ("There are no files in the directory")
            return
        
        print ("Proceeding to delete previous files and sub directories")
        for item in os.listdir(fileSetDirectory):
            #item is just a name, not the full path
            itemFullpath = path.join(fileSetDirectory,item)
            if path.isfile(itemFullpath):
                os.remove(itemFullpath)
                continue
            if path.isdir(itemFullpath):
                shutil.rmtree(itemFullpath)
                continue


    #*****************************************
    # internalGetDirectoryName
    #*****************************************
    def internalGetDirectoryName(self):
        return path.join(self.rootDirectory
            ,self.fileSetDirectoryName)

    #*****************************************
    # public: getDirectory
    #*****************************************
    #Only public method
    #Make sure the directory exists
    #and that it is empty
    def getDirectory(self):

        #If the directory is already validated
        #return it.
        if self.validatedFlag == True:
            return self.internalGetDirectoryName()

        #It is not validated
        #it is the first use
        if self.firstUseFlag == True:
            #it is the first time
            self.firstUseFlag = False
            if self.validateOnFirstUse() == True:
                self.validatedFlag = True
            
        #If the directory is already validated
        #return it.
        if self.validatedFlag == True:
            return self.internalGetDirectoryName()

        #Something went wrong preparing the directory
        raise RuntimeError("Something went wrong preparing the directory: {}"
            .format(self.internalGetDirectoryName()))

#*********************************************
# Just a utility function that converts
# an ISO string to a filename based on the hour
#*********************************************
#Date format: 2019-09-01 00:05:00.000

def getFilenameByHour(fileBasename, datetimeInstanceString):
    #i - instance
    i = datetime.fromisoformat(datetimeInstanceString)

    #see the formating options for padding 
    #integers are padded with 0s
    filePostFix = (
        "{}{:02d}{:02d}{:02d}"
        .format(i.year, i.month,i.day, i.hour)
    )
    return fileBasename + filePostFix + ".csv"

def getTheHour(isoDatetimeInstanceString):
    i = datetime.fromisoformat(isoDatetimeInstanceString)
    return i.hour


def testOutputFilesetDirectory():
    #Test this class
    rootDirectory = r"C:\satya\data\code\pyspark\data\testdata"
    fileSetDirectory="testFileDirectory"

#Uncomment while testing this
#testThis();
#*****************************************
# Possible test cases
#
# 1. root directory is not a directory
# 2. fileset directory doesn't exist
# 3. fileset directory has no files
# 4. fileset directory has files
# 5. getdirectory is called multipple times
# 6. validateOnFirstUse should be called only once
#*****************************************

