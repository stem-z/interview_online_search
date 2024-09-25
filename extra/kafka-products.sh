kafka-topics --bootstrap-server localhost:9092 --topic product-descriptions --create --partitions 1 --replication-factor 1
kafka-console-producer --bootstrap-server localhost:9092 --topic product-descriptions < ./extra/kafka-messages.txt
