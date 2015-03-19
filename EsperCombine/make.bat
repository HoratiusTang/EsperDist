@echo off
#del bin\*.class /S /Q
@echo on

call .\make_kryonet.bat

javac .\core\combine\esper\core\CoordinatorMain.java -sourcepath .\core;.\kryonet -d .\bin -classpath .\bin;.\lib\*;..\EsperDist2\lib\*;..\EsperDist2\lib\commons\*;..\EsperDist2\lib\kryonet\*;..\EsperDist2\lib\sigar-bin\lib\*;..\EsperDist2\lib\esper-5.0.0\esper\*;..\EsperDist2\lib\esper-5.0.0\esperio-amqp\*;..\EsperDist2\lib\esper-5.0.0\esperio-http\*;..\EsperDist2\lib\esper-5.0.0\esper-springjms\*;..\EsperDist2\lib\esper-5.0.0\esperio-stax\*;..\EsperDist2\lib\util\*

javac .\core\combine\esper\core\WorkerMain.java -sourcepath .\core;.\kryonet -d .\bin -classpath .\bin;.\lib\*;..\EsperDist2\lib\*;..\EsperDist2\lib\commons\*;..\EsperDist2\lib\kryonet\*;..\EsperDist2\lib\sigar-bin\lib\*;..\EsperDist2\lib\esper-5.0.0\esper\*;..\EsperDist2\lib\esper-5.0.0\esperio-amqp\*;..\EsperDist2\lib\esper-5.0.0\esperio-http\*;..\EsperDist2\lib\esper-5.0.0\esper-springjms\*;..\EsperDist2\lib\esper-5.0.0\esperio-stax\*;..\EsperDist2\lib\util\*

javac .\test\combine\esper\test\TestSimulation3.java -sourcepath .\core;.\kryonet -d .\bin -classpath .\bin;.\lib\*;..\EsperDist2\lib\*;..\EsperDist2\lib\commons\*;..\EsperDist2\lib\kryonet\*;..\EsperDist2\lib\sigar-bin\lib\*;..\EsperDist2\lib\esper-5.0.0\esper\*;..\EsperDist2\lib\esper-5.0.0\esperio-amqp\*;..\EsperDist2\lib\esper-5.0.0\esperio-http\*;..\EsperDist2\lib\esper-5.0.0\esper-springjms\*;..\EsperDist2\lib\esper-5.0.0\esperio-stax\*;..\EsperDist2\lib\util\*