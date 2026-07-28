# Create SCRAM user 'app' for SASL authentication
docker exec -it kafka-1 /opt/kafka/bin/kafka-configs.sh \
  --alter \
  --add-config 'SCRAM-SHA-256=[password=supersecret]' \
  --entity-type users \
  --entity-name app \
  --bootstrap-server kafka-1:9092

# Create SCRAM user 'admin' for SASL authentication
docker exec -it kafka-1 /opt/kafka/bin/kafka-configs.sh \
  --alter \
  --add-config 'SCRAM-SHA-256=[password=password]' \
  --entity-type users \
  --entity-name admin \
  --bootstrap-server kafka-1:9092

# Create secure topic for Confluent client SASL test
docker exec -it kafka-1 /opt/kafka/bin/kafka-topics.sh \
  --create \
  --topic secure-app-confluent-topic \
  --partitions 1 \
  --replication-factor 3 \
  --bootstrap-server kafka-1:29092

# Create secure topic for Segmentio Go client SASL test
docker exec -it kafka-1 /opt/kafka/bin/kafka-topics.sh \
  --create \
  --topic secure-app-segmentio-topic \
  --partitions 1 \
  --replication-factor 3 \
  --bootstrap-server kafka-1:29092
