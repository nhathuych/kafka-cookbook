# Create a new Kafka topic with a replication factor of 3 across the cluster
docker exec -it kafka-1 /opt/kafka/bin/kafka-topics.sh --create --topic multi-node-test --partitions 1 --replication-factor 3 --bootstrap-server kafka-1:29092

# Display the configuration and partition/replica details of the Kafka topic
docker exec -it kafka-1 /opt/kafka/bin/kafka-topics.sh --describe --topic multi-node-test --bootstrap-server kafka-1:29092

# Display a list of all Kafka topics in the cluster
docker exec -it kafka-1 /opt/kafka/bin/kafka-topics.sh --list --bootstrap-server kafka-1:29092

# Start a Kafka producer on broker kafka-3 to publish messages to the topic
docker exec -it kafka-3 /opt/kafka/bin/kafka-console-producer.sh --topic multi-node-test --bootstrap-server kafka-3:29092

# Start a Kafka consumer on broker kafka-2 and read all messages from the beginning of the topic
docker exec -it kafka-2 /opt/kafka/bin/kafka-console-consumer.sh --topic multi-node-test --from-beginning --bootstrap-server kafka-2:29092
