# Create topic for Kafka Transactions Demo
docker exec -it kafka-1 /opt/kafka/bin/kafka-topics.sh \
  --create \
  --topic order-events-txn \
  --partitions 1 \
  --replication-factor 3 \
  --bootstrap-server kafka-1:29092

# Create topic for Payment Transactional Events
docker exec -it kafka-1 /opt/kafka/bin/kafka-topics.sh \
  --create \
  --topic payment-events-txn \
  --partitions 1 \
  --replication-factor 3 \
  --bootstrap-server kafka-1:29092

# Consume committed transactions only from order-events-txn
docker exec -it kafka-1 /opt/kafka/bin/kafka-console-consumer.sh \
  --topic order-events-txn \
  --from-beginning \
  --consumer-property isolation.level=read_committed \
  --bootstrap-server kafka-1:29092

# Consume committed transactions only from payment-events-txn on kafka-2
docker exec -it kafka-2 /opt/kafka/bin/kafka-console-consumer.sh \
  --topic payment-events-txn \
  --from-beginning \
  --consumer-property isolation.level=read_committed \
  --bootstrap-server kafka-2:29092
