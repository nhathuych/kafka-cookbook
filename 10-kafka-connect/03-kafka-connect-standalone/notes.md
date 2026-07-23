# Create a Kafka topic for the File Stream Source Connector
docker exec -it kafka-1 /opt/kafka/bin/kafka-topics.sh \
  --create \
  --topic file-stream-topic \
  --partitions 1 \
  --bootstrap-server kafka-1:29092



# Start a temporary Kafka Connect worker in standalone mode.
# Mount the connector configuration and input file into the container.
cd 10-kafka-connect/03-kafka-connect-standalone

docker run --rm -it \
  --name connect-worker \
  --network 10-kafka-connect_default \
  -v ./connect-standalone.properties:/tmp/connect-standalone.properties \
  -v ./file-source.properties:/tmp/file-source.properties \
  -v ./input.txt:/tmp/input.txt \
  apache/kafka:4.0.0 /opt/kafka/bin/connect-standalone.sh \
  /tmp/connect-standalone.properties \
  /tmp/file-source.properties



# Consume messages from the beginning to verify that the connector
# successfully streams each line from input.txt into Kafka.
docker exec -it kafka-1 /opt/kafka/bin/kafka-console-consumer.sh \
  --topic file-stream-topic \
  --from-beginning \
  --bootstrap-server kafka-1:29092



# ⚠️ IMPORTANT:
# Always press Enter to create a final newline before saving input.txt.
# Otherwise, the last line will not be sent to Kafka.
