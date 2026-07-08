# Create topic "user-profiles-segio" with 3 partitions, replication factor 3, min.insync.replicas=2 and cleanup.policy=delete
docker exec -it kafka-1 /opt/kafka/bin/kafka-topics.sh --create \
  --topic user-profiles-segio \
  --partitions 3 \
  --replication-factor 3 \
  --config min.insync.replicas=2 \
  --config cleanup.policy=delete \
  --bootstrap-server kafka-1:29092
