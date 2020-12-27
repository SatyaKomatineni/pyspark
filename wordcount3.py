###############################################
# RDD APIs tested:
#   flatmap
#   map
#   sort
#   collect
#
# Others:
#  type(), len()
#  string.split(), .lower(), .format()
#
# General reminders in Python
#  Things inside () are tuples
#  Things inside [] are lists
#
#  Key libraries used
#  pyspark.sql
#  pyspark.rdd
#
# Some types used:
#
# pyspark.sql.session.SparkSession
# pyspark.sql.dataframe.DataFrame
# pyspark.rdd.RDD
# pyspark.rdd.PipelinedRDD
###############################################

from __future__ import print_function

import sys
from operator import add

from pyspark.sql import SparkSession
from os import path

#***********************************
# Definitions
#***********************************
dataDir = r"C:\satya\data\code\pyspark"
appName = "PythonWordCount"

#***********************************
#Function: printCollected
#***********************************
def printBeginMsg(msg):
    print ("*****************************")
    print ("*" + msg)
    print ("*****************************")

def printEndMsg(msg):
    print ("*****************************")
    print ("* End of " + msg)
    print ("*****************************")

def printType(varname: str, obj):
    print("Type of {}:{}".format(varname,type(obj)))

#RDDs are distributed and stay distributed
#until a collect() (or another function that require gathering) is called
#This is a utility function for this demo only
#In practicce collect is called very few times
def printCollected(msg, rdd):
    rddCollected = rdd.collect()
    printBeginMsg(msg)
    for item in rddCollected:
        print (item)
    printEndMsg(msg)


def getASampleSonnetFile():
    return (dataDir + r"\sonnet2.txt")

def getFullSonnetFilePath(filename):
    return path.join(dataDir,filename)

def getSparkSession(appName):
    return SparkSession.builder.appName(appName).getOrCreate()

#***********************************
#End of function
#***********************************

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: wordcount <file>", file=sys.stderr)
        sys.exit(-1)

    #Give a name to this application and call it a session
    spark = getSparkSession(appName)

    #get a text file to read
    sonnetFilename = getASampleSonnetFile()

    #***********************************
    #Get a Reader
    #***********************************
    #dfr: pyspark.sql.readwriter.DataFrameReader
    #prepare to read
    dfr = spark.read

    #***********************************
    #Read lines from a file: into a DataFrame
    #***********************************
    #lines: pyspark.sql.dataframe.DataFrame
    #functions include text, csv, json, parquet etc
    #each to read the respective files into a data frame
    #Schema is implied based on each file type

    #The way a text file is read: each line becomes a record or row
    #such a record will have 1 column whose name is value
    #With additional options you can change this behavior (like read the whole file)
    lines = dfr.text(sonnetFilename)

    #For a text file the schema is as below
    #1 row per line
    #it has one column for each whole line. Column is called value
    
    #***********************************
    #Get an RDD from the DataFrame
    #Perhaps you can work with DF directly. But for now ....
    #***********************************
    #pyspark.rdd.RDD
    linesAsRDD = dfr.rdd

    #***********************************
    # Collect to be able to print etc
    #***********************************
    #This becomes a list
    collectedRDDLines = linesAsRDD.collect()
    #python list of Row objects
    printType("CollectedRDDLines",collectedRDDLines)

    #***********************************
    # map an RDD: similar to select
    #***********************************
    #pyspark.rdd.PipelinedRDD
    #These are a list of strings as opposed to collection of rows and columns
    #Need to understand what a PipelinedRDD is then
    mappedLinesRDD = linesAsRDD.map(lambda r: r[0])
    printType("mappedLinesRDD",mappedLinesRDD)
    printCollected("Raw lines", lines)

    #you can also do for the same effect
    #where value is the column on the row object
    mappedLinesRDD = linesAsRDD.map(lambda row: row.value)

    #***********************************
    # Break the lines into words: map, flatmap
    #***********************************
    lineAsListOfWords = lines.map(lambda x: x.split(' ')) 
    #pyspark.rdd.PipelinedRDD
    printType("lineAsListOfWords",lineAsListOfWords)

    printCollected("Raw lines split into words. Each line is a list of words", \
        lineAsListOfWords)

    justAListOfWords = lineAsListOfWords.flatMap(lambda x: x).map(lambda x: x.lower())
    printCollected("Just A List of Words, from flatmap", justAListOfWords)

    #***************************************************************
    # make each word a list which is (word, length-of-thatword)
    # WordObject: (word, length, howmany)
    #***************************************************************
    listOfWordObjects = justAListOfWords.map(lambda x: (x, len(x), 1))
    printCollected("List of Word Objects", listOfWordObjects)

    #***************************************************************
    # Even though there are duplicated words each words length is 
    # determined multiple times.
    # Lets reduce the list first
    # WordObject2: (word, howmany)
    #***************************************************************
    listOfWordObjects2 = justAListOfWords.map(lambda x: (x, 1))
    printCollected("List of Word Objects 2, no length", listOfWordObjects2)

    #***************************************************************
    #Lets count the similarwords together
    #Although I call it a list, it is really an RDD
    #The lambda function returns a tuple
    #likely the map on the RDD will convert that to an RDD again
    #So the output is really not a python object but spark object
    #***************************************************************
    listOfUniqueWordObjects2RDD = listOfWordObjects2.reduceByKey(add)
    printCollected("List of Unique Word Objects 2, no length", listOfUniqueWordObjects2RDD)

    #***************************************************************
    #This will print 
    #Type of .map on an RDD is: <class 'pyspark.rdd.PipelinedRDD'>
    #***************************************************************
    print("Type of .map on an RDD is: {}".format(type(listOfUniqueWordObjects2RDD)));

    #***************************************************************
    # WordObject3: (word, length, howmany)
    #***************************************************************
    listOfUniqueWordObjects3RDD = listOfUniqueWordObjects2RDD.map(lambda x: (x[0], len(x[0]), x[1]))
    printCollected("List of Unique Word Objects 3, with length", listOfUniqueWordObjects3RDD)

    #***************************************************************
    #sort words by length
    #***************************************************************
    listOfUniqueWordObjects3SoryByLength = \
        listOfUniqueWordObjects3RDD.sortBy(\
            lambda x: (x[1]), False)
    printCollected("List of Unique Word Objects sorted by length", \
        listOfUniqueWordObjects3SoryByLength)

    #***************************************************************
    #sort words by Frequency
    #***************************************************************
    listOfUniqueWordObjects3SoryByFreq = \
        listOfUniqueWordObjects3RDD.sortBy(\
            lambda x: (x[2]), False)
    printCollected("List of Unique Word Objects sorted by frequency", \
        listOfUniqueWordObjects3SoryByFreq)

    spark.stop()

