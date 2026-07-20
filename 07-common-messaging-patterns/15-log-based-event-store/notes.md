# Topic creation: create the topic used for storing audit logs.
docker exec -it kafka-1 /opt/kafka/bin/kafka-topics.sh --create \
  --topic audit-log-topic \
  --partitions 1 \
  --replication-factor 3 \
  --bootstrap-server kafka-1:29092

# Console producer: start an interactive producer to publish audit log events to the cluster.
docker exec -it kafka-1 /opt/kafka/bin/kafka-console-producer.sh \
  --topic audit-log-topic \
  --bootstrap-server kafka-1:29092,kafka-2:29092,kafka-3:29092

# Console consumer: start a consumer belonging to a consumer group to read and print audit logs.
docker exec -it kafka-1 /opt/kafka/bin/kafka-console-consumer.sh \
  --topic audit-log-topic \
  --bootstrap-server kafka-1:29092,kafka-2:29092,kafka-3:29092 \
  --group audit-log-consumer-group

# Reprocessing consumer: start a consumer in a separate group to read all audit logs from the beginning for reprocessing.
docker exec -it kafka-1 /opt/kafka/bin/kafka-console-consumer.sh \
  --topic audit-log-topic \
  --bootstrap-server kafka-1:29092,kafka-2:29092,kafka-3:29092 \
  --group reprocessing-consumer-group \
  --from-beginning
