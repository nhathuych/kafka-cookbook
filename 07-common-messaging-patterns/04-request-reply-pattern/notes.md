# Request topic: producers publish fraud-check requests for asynchronous processing.
docker exec -it kafka-1 /opt/kafka/bin/kafka-topics.sh --create \
  --topic fraud-check-requests \
  --partitions 1 \
  --replication-factor 3 \
  --bootstrap-server kafka-1:29092

# Reply topic: consumers send the corresponding responses back using the request-reply pattern.
docker exec -it kafka-1 /opt/kafka/bin/kafka-topics.sh --create \
  --topic payment-service-replies \
  --partitions 1 \
  --replication-factor 3 \
  --bootstrap-server kafka-1:29092
