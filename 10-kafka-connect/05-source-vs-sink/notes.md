# Create topic for File Sink Connector Pattern
docker exec -it kafka-1 /opt/kafka/bin/kafka-topics.sh \
  --create \
  --topic sink-input-topic \
  --partitions 1 \
  --replication-factor 3 \
  --bootstrap-server kafka-1:29092

# Register File Sink Connector via REST API
curl -X POST -H "Content-Type: application/json" --data '@file-sink-config.json' http://localhost:8083/connectors

# Produce test data to sink-input-topic
docker exec -it kafka-1 /opt/kafka/bin/kafka-console-producer.sh \
  --topic sink-input-topic \
  --bootstrap-server kafka-1:29092

# Verify File Sink output
docker exec -it connect-distributed tail -f /tmp/output.txt
