@echo off

@rem the spark examples are at
@rem c:\satya\i\spark\examples\src\main\python
@rem notice the spark bin directory in its installation path

@rem *****************************************************
@rem this is how to submit a spark job using .py file
@rem example: rs1.cmd wordcount.py sonnets2.txt
@rem 
@rem rs1.cmd : This batch file
@rem wordcount.py : pyspark program
@rem sonnet2.txt: input argument
@rem
@rem pwd: C:\satya\data\code\pyspark 
@rem     \wordcount.py
@rem     \sonnet2.txt
@rem
@rem *****************************************************
c:\satya\i\spark\bin\spark-submit --master local[4] %1 %2 %3