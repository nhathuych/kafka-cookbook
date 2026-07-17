# Transaction topic: handles demo transaction events
docker exec -it kafka-1 /opt/kafka/bin/kafka-topics.sh --create \
  --topic transactions-demo \
  --partitions 1 \
  --replication-factor 3 \
  --bootstrap-server kafka-1:29092
