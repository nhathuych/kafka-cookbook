# Create topic for StringConverter demo
docker exec -it kafka-1 /opt/kafka/bin/kafka-topics.sh \
  --create \
  --topic string-topic \
  --partitions 1 \
  --replication-factor 3 \
  --bootstrap-server kafka-1:29092

# Register File Source with StringConverter
curl -X POST -H "Content-Type: application/json" --data '@string-source-config.json' http://localhost:8083/connectors

# Consume StringConverter output as plain string
docker exec -it kafka-1 /opt/kafka/bin/kafka-console-consumer.sh \
  --topic string-topic \
  --from-beginning \
  --bootstrap-server kafka-1:29092

# Delete StringConverter connector
curl -X DELETE http://localhost:8083/connectors/string-file-source/

# Create topic for JsonConverter demo
docker exec -it kafka-1 /opt/kafka/bin/kafka-topics.sh \
  --create \
  --topic json-topic \
  --partitions 1 \
  --replication-factor 3 \
  --bootstrap-server kafka-1:29092

# Register File Source with JsonConverter
curl -X POST -H "Content-Type: application/json" --data '@json-source-config.json' http://localhost:8083/connectors

# Consume JsonConverter output as JSON
docker exec -it kafka-1 /opt/kafka/bin/kafka-console-consumer.sh \
  --topic json-topic \
  --from-beginning \
  --bootstrap-server kafka-1:29092
