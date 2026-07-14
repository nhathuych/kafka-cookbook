# Create a new Kafka topic with a replication factor of 3 across the cluster
docker exec -it kafka-1 /opt/kafka/bin/kafka-topics.sh --create --topic town-announcements --partitions 1 --replication-factor 3 --bootstrap-server kafka-1:29092

# Start a Kafka consumer in the analytics-group consumer group and read all messages from the beginning of the topic
docker exec -it kafka-1 /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka-1:29092 --topic town-announcements --group analytics-group --from-beginning

# Start a Kafka consumer in the email-group consumer group and read all messages from the beginning of the topic
docker exec -it kafka-2 /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka-2:29092 --topic town-announcements --group email-group --from-beginning
