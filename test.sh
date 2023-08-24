# blocks until kafka is reachable
echo -e 'Listing kafka topics'
kafka-topics --bootstrap-server kafka-1:9092 --list

echo -e 'Creating kafka topics'
kafka-topics --bootstrap-server kafka-1:9092 --create --if-not-exists --topic user-information --replication-factor 3 --partitions 3
sleep 2


kafka-topics --bootstrap-server kafka-1:9092 --create --if-not-exists --topic fines --replication-factor 3 --partitions 5
sleep 2


kafka-topics --bootstrap-server kafka-1:9092 --create --if-not-exists --topic user-fines-information --replication-factor 3 --partitions 4
sleep 2

echo -e 'Successfully created the following topics:'
kafka-topics --bootstrap-server kafka-1:9092 --list

sleep 5

echo -e 'Reading data from topic'
kafka-console-consumer --bootstrap-server=kafka-1:9092 --topic user-information --from-beginning


