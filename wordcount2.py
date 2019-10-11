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
###############################################

from __future__ import print_function

import sys
from operator import add

from pyspark.sql import SparkSession

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

#***********************************
#End of function
#***********************************

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: wordcount <file>", file=sys.stderr)
        sys.exit(-1)

    spark = SparkSession\
        .builder\
        .appName("PythonWordCount")\
        .getOrCreate()


    #
    # spark.read is a property
    # it returns a dataframereader
    #
    lines = spark.read.text(sys.argv[1]).rdd.map(lambda r: r[0])
    printCollected("Raw lines", lines)

    lineAsListOfWords = lines.map(lambda x: x.split(' ')) 
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

