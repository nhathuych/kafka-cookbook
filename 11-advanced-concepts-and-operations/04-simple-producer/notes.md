# Create topic for Go App Producer
docker exec -it kafka-1 /opt/kafka/bin/kafka-topics.sh \
  --create \
  --topic go-app-events \
  --partitions 1 \
  --replication-factor 3 \
  --bootstrap-server kafka-1:29092
