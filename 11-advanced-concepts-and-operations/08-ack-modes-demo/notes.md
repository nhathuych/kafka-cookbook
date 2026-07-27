# Create topic for Acks Modes Demo with min.insync.replicas=2
docker exec kafka-1 /opt/kafka/bin/kafka-topics.sh \
  --create \
  --topic acks-demo-topic \
  --partitions 1 \
  --replication-factor 3 \
  --config min.insync.replicas=2 \
  --bootstrap-server kafka-1:29092

# Describe acks-demo-topic configuration
docker exec -it kafka-1 /opt/kafka/bin/kafka-topics.sh \
  --describe \
  --topic acks-demo-topic \
  --bootstrap-server kafka-1:29092
