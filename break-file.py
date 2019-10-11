#
# Break a file sequentially
#
#

#Some constants
dataDirectory = r'C:\satya\data\code\pyspark'

processedDir = dataDirectory \
    + r"\data\mp_scada_data\processed"

inputWuActualWeatherForAWeek = \
    dataDirectory \
    + r"\data\mp_scada_data\wu_actual\All Data.csv"


def printVar(msg, var1):
    formatMsg = msg + ": {}"
    finalMsg = formatMsg.format(var1)
    print(finalMsg)

printVar ("Data directory", dataDirectory)
printVar ("inputWuActualWeatherForAWeek",inputWuActualWeatherForAWeek)

#You can also do this instead
print ("Data directory: ", dataDirectory)
print ("inputWuActualWeatherForAWeek: ",inputWuActualWeatherForAWeek)

#This file object will be closed on exit
inputFileObj = open(inputWuActualWeatherForAWeek)


#Given a date since eon return a filename
def getFilenameForDate(date):
    basename="blah"
    #basname-YYYYMMDDHR.csv
    return ""

#Local utility function
def printList(inList):
    if len(inList) < 10:
        print (inList)
    else:
        print ("List too big")
        print (len(inList))

count = 0

def getHourlyList(line, lastCall=False):
    lastLine = None
    lastList = []


#Date format: 2019-09-01 00:05:00.000
#List of lines in the ifle
for idx, line in enumerate(inputFileObj) :
    #Exclude line 1
    if (idx == 1): continue

    #If more than 20 lines break
    if (idx > 5): break

    tempList = getHourlyList(line)
    if tempList == false:
        continue
    else:
        print (tempList)



print("pringing mylist")
printList(mylist)


