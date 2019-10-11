#
# *************************************************
# Break a weekly worth of data file to 
# many files by the hour.
#
# 24 x 7 = 140 + 28 = 168 files
# 13 rows in each file (first row header)
#
# *************************************************
#

#import datetime obect from datetime standard library
from datetime import datetime
import os
import sys

#print (sys.path)

#from .outputfilesetdirectory  import *
#import .outputfilesetdirectory as fileutils
#from outputfilesetdirectory import getTheHour
from outputfilesetdirectory import getTheHour


#Some constants
dataDirectory = r'C:\satya\data\code\pyspark'

processedDir = dataDirectory \
    + r"\data\mp_scada_data\processed"

#*************************************
#Actual weather for a week
#Full path Filename
#*************************************
inputWuActualWeatherForAWeek = \
    dataDirectory \
    + r"\data\mp_scada_data\wu_actual\All Data.csv"


#Start the program
print ("Getting ready to break file:", inputWuActualWeatherForAWeek)
print ("Data directory:", dataDirectory)
print ("inputWuActualWeatherForAWeek:",inputWuActualWeatherForAWeek)

#return if the file does not exist
if os.path.exists(inputWuActualWeatherForAWeek) == False:
    print ("File does not exist:", inputWuActualWeatherForAWeek)
    exit(1)

#file exists
#Lets define a generator that breaks up the rows
#by the hour.

#*********************************************
# RowsByTheHourGenerator
# *************************
# 1. Read each line in a file (like a cursor)
# 2. Eliminate lines that needs eliminating
# 3. Present the results as if this is an iterator
#*********************************************
def RowsByTheHourGenerator(inFilename):
    #remember filename
    filename = inFilename
    print("opening file:", inFilename)
    inputFileObj = open(inFilename)

    #initialize this later when the first row is read
    firstRow = None
    curHour = 0
    curLine = None

    #A list to hold an hours worth of rows
    listByTheHour = []

    for idx, line in enumerate(inputFileObj) :
        #ignore first line
        if (idx == 0): 
            print("Gathering first row")
            firstRow = line
            listByTheHour.append(firstRow)
            continue

        #Split the line
        fieldList = line.split(",")
        isoDateTime = fieldList[0]
        #for each line: only do 6 lines
        thisHour = getTheHour(isoDateTime)

        if thisHour != curHour :
            #Different hour
            #yield the previous list if not empty
            yield listByTheHour

            #Reset the variables
            print("Resetting the list after yield")
            curHour = thisHour
            listByTheHour.clear()
            listByTheHour.append(firstRow)
            listByTheHour.append(line)
            continue

        #it is the same hour            
        listByTheHour.append(line)

    #End of for
    if len(listByTheHour) > 1:
        #something in the list
        print("Returning last list")
        return listByTheHour
    else:
        print("No more rows in the list")

#********************************************
#Test this gen
#********************************************
def testGenFirst():
    hourlyGen = RowsByTheHourGenerator(inputWuActualWeatherForAWeek)

    for idx, hourlyList in enumerate(hourlyGen):
        if idx > 2: break
        print("There are {} rows in this list".format(len(hourlyList)))
        #print(hourlyList)

def testImport():
    hour = getTheHour("2019-09-01 00:05:00.000")
    print("Gotten hour is:", hour)

#********************************************
#Continue with the script
#********************************************
testImport()
print (sys.path)