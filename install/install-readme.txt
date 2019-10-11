
set install-dir=c:\satya\i

@rem ********************************
@rem Disable ipv6
@rem ********************************
setx /M _JAVA_OPTIONS "-Djava.net.preferIPv4Stack=true"

@rem ********************************
@rem Set java stuff
@rem ********************************
setx /M JAVA_HOME "%install-dir%\jdk12"


set java-path=%install-dir%\jdk12\bin
set python-path=%install-dir%\python374
set hadoop-path=%install-dir%\hadoop312-common-bin\bin


@rem python: c:\satya\i\python374
@rem hadoop: c:\satya\i\hadoop312-common-bin\bin

echo %java-path%;%python-path%;%hadoop-path%


System level Environment variables:

#***************************************
#Java options to disable ipv6
#***************************************
_JAVA_OPTIONS "-Djava.net.preferIPv4Stack=true"
JAVA_HOME "%install-dir%\jdk12"
HADOOP_HOME C:\satya\i\hadoop312-common-bin

#***************************************
#Add the following paths to the system path variable
#***************************************
#Java path
%install-dir%\jdk12\bin

#python-path
%install-dir%\python374

#hadoop-path
%install-dir%\hadoop312-common-bin\bin


I have 2 pythons installed:

One from the app store is at:

C:\Users\satya\AppData\Roaming\Python\Python37\Scripts


I don't see anyone in the paths.

examples\src\main\python\wordcount.py

spark/bin/spark-submit --master local[4] SimpleApp.py

..\bin\spark-submit --master local[4] ..\examples\src\main\python\wordcount.py

c:\satya\i\spark\bin\spark-submit --master local[4] c:\satya\i\spark\examples\src\main\python\wordcount.py