./clean_bin.sh
./make_kryonet.sh

javac ./core/dist/esper/core/CoordinatorMain.java -sourcepath ./core:./io:./external:./experiment:./kryonet -d ./bin -classpath ./bin:./lib/*:./lib/commons/*:./lib/kryonet/*:./lib/sigar-bin/lib/*:./lib/esper-5.0.0/esper/*:./lib/esper-5.0.0/esperio-amqp/*:./lib/esper-5.0.0/esperio-http/*:./lib/esper-5.0.0/esper-springjms/*:./lib/esper-5.0.0/esperio-stax/*:./lib/util/* 

javac ./core/dist/esper/core/WorkerMain.java -sourcepath ./core:./io:./external:./experiment:./kryonet -d ./bin -classpath ./bin:./lib/*:./lib/commons/*:./lib/kryonet/*:./lib/sigar-bin/lib/*:./lib/esper-5.0.0/esper/*:./lib/esper-5.0.0/esperio-amqp/*:./lib/esper-5.0.0/esperio-http/*:./lib/esper-5.0.0/esper-springjms/*:./lib/esper-5.0.0/esperio-stax/*:./lib/util/*

javac ./external/dist/esper/external/SpoutMain.java -sourcepath ./core:./io:./external:./experiment:./kryonet -d ./bin -classpath ./bin:./lib/*:./lib/commons/*:./lib/kryonet/*:./lib/sigar-bin/lib/*:./lib/esper-5.0.0/esper/*:./lib/esper-5.0.0/esperio-amqp/*:./lib/esper-5.0.0/esperio-http/*:./lib/esper-5.0.0/esper-springjms/*:./lib/esper-5.0.0/esperio-stax/*:./lib/util/*

javac ./test/dist/esper/test/TestSimulation3.java -sourcepath ./core:./io:./external:./experiment:./kryonet -d ./bin -classpath ./bin:./lib/*:./lib/commons/*:./lib/kryonet/*:./lib/sigar-bin/lib/*:./lib/esper-5.0.0/esper/*:./lib/esper-5.0.0/esperio-amqp/*:./lib/esper-5.0.0/esperio-http/*:./lib/esper-5.0.0/esper-springjms/*:./lib/esper-5.0.0/esperio-stax/*:./lib/util/*
