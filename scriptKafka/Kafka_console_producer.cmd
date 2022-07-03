:: #Démarrage console Producer :
%KAFKA_HOME%\bin\windows\kafka-console-producer.bat --broker-list localhost:9092 --topic DetailsCommande --property "parse.key=true" --property "key.separator=::"