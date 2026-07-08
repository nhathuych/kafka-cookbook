# Create a new Kafka topic named "user-profiles-conf" with 4 partitions and a replication factor of 3 across the cluster
docker exec -it kafka-1 /opt/kafka/bin/kafka-topics.sh --create \
  --topic user-profiles-conf \
  --partitions 4 \
  --replication-factor 3 \
  --bootstrap-server kafka-1:29092
