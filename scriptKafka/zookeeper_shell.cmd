:: #lancement du shell de ZooKeeper :
%KAFKA_HOME%\bin\windows\zookeeper-shell.bat localhost:2181 ls /brokers/topics
ls /brokers/topics
deleteall /brokers/topics/helloWorld-counts-store-repartition
deleteall /brokers/topics/helloWorld-counts-store-changelog
ls /brokers/topics