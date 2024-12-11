REM BEFORE RUNNING PLEASE MAKE SURE THAT PATHS IN THE
REM PORTION "UPDATE HERE" POINT TO RIGHT LOCATIONS
REM IT CREATES VIRUAL ENVIR IF IT IS MISSING 

REM WHEN VS CODE ASKS FOR INTERPRETER CHOOSE ONE 
REM FROM THE .venv\Scripts LOCATION   

REM ---------------------------------------------
REM ++++++++++++++ UPDATE HERE ++++++++++++++++++

set "SPARK_HOME=C:\apache_spark\spark"

REM bin folder location of the winutils.exe and hadoop.ddl
REM https://cwiki.apache.org/confluence/display/HADOOP2/WindowsProblems
set "HADOOP_HOME=C:\apache_spark\hadoop"

REM https://stackoverflow.com/questions/30824818/what-to-set-spark-home-to
set "JAVA_HOME=C:\Program Files\Java\jdk1.8.0_202"  

REM IF you wnat to see the code
REM set "PY_SPARK_SRC="
REM PYSPARK SOURCE CODE very werid but it is necesary, sometimes it crashes, make sure that version is correct
set "PY_SPARK_SRC=%SPARK_HOME%\python\lib\py4j-0.10.9-src.zip"
                            
REM ++++++++++++++ END UPDATE +++++++++++++++++++
REM ---------------------------------------------

set "VIRTUAL_ENV=.venv"
set "PATH=%VIRTUAL_ENV%\Scripts;%PATH%"

set "PYSPARK_PYTHON=%VIRTUAL_ENV%\Scripts\python.exe"
set "PYSPARK_DRIVER_PYTHON=%PYSPARK_PYTHON%"
set "REQ_FILE=requirements.txt"

IF EXIST "%PYSPARK_PYTHON%" (
    echo "venv is setup moving forward!"
) ELSE (
    echo "Lets setup new venv ..."
    python -m venv %VIRTUAL_ENV%

IF EXIST "%REQ_FILE%" (
        echo "Lets setun packages based on %REQ_FILE% file!"
        pip install -r %REQ_FILE%
) ELSE (
        echo "No %REQ_FILE% was provided install packs mannualy" 
)
)
REM Make project code, spark visible for the tests and internal code # %PY_SPARK_SRC%;
set "PYTHONPATH=src;%SPARK_HOME%\python;%SPARK_HOME%\python\lib\pyspark.zip;%PYTHONPATH%" 

REM https://stackoverflow.com/questions/60257377/encountering-warn-procfsmetricsgetter-exception-when-trying-to-compute-pagesi
set "PATH=%SPARK_HOME%;%SPARK_HOME%\python;%PY_SPARK_SRC%;%SPARK_HOME%\python\lib\pyspark.zip;%PYTHONPATH%;%HADOOP_HOME%\bin;%JAVA_HOME%\bin;%PATH%"


START /B /wait code .
REM CALL  /B /wait code .

REM Create “New” > Variable name: PYSPARK_PYTHON > and then Browse Directory for the python.exe path
REM Create “New” > Variable name: PYSPARK_DRIVER_PYTHON> and then Browse Directory for the python.exe path

REM t's better to add this to $SPARK_HOME/conf/spark-env.sh so spark-submit uses the same interpreter
REM https://stackoverflow.com/questions/23572724/why-does-bin-spark-shell-give-warn-nativecodeloader-unable-to-load-native-had




