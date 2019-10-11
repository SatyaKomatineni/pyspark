#A quick datetime example 
#2019-09-01 00:05:00.000

#import datetime obect from datetime standard library
from datetime import datetime

#Take an example ISO formatted datetime string
sampleTimeInstanceString = "2019-09-01 00:05:00.000"

#Create a datetime instance
#Notice the spreading args on multile lines :)
someInstance = datetime.fromisoformat(
    sampleTimeInstanceString
    )

#Examine the datetime object
#VSCode with pyright for intellisense
print(someInstance)
print(someInstance.year)
print(someInstance.month)
print(someInstance.day)

#Lets do something useful with the datetime object
#Create a filename that is specific to the hour 
#of that time instance
def getFilename(datetimeInstanceString):
    fileBasename = "somebase-filename-for-"
    #i - instance
    i = datetime.fromisoformat(datetimeInstanceString)

    #see the formating options for padding 
    #integers are padded with 0s
    filePostFix = (
        "{}{:02d}{:02d}{:02d}"
        .format(i.year, i.month,i.day, i.hour)
    )
    return fileBasename + filePostFix + ".csv"

print (getFilename(sampleTimeInstanceString))
# will print the following
# somebase-filename-for-2019090100.csv
