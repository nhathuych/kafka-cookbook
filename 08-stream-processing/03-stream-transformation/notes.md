# Source topic for Stream Transformations
# Input events are consumed from this topic before applying transformations
docker exec -it kafka-1 /opt/kafka/bin/kafka-topics.sh --create \
  --topic raw-user-events \
  --partitions 1 \
  --bootstrap-server kafka-1:29092

# Output topic for Stream Transformations
# Transformed events are published to this topic after processing
docker exec -it kafka-1 /opt/kafka/bin/kafka-topics.sh --create \
  --topic processed-user-events \
  --partitions 1 \
  --bootstrap-server kafka-1:29092

# Producer for Raw Events Topic
# Opens a console producer CLI to manually send input test messages into the source topic
docker exec -it kafka-1 /opt/kafka/bin/kafka-console-producer.sh \
  --topic raw-user-events \
  --bootstrap-server kafka-1:29092

# Consumer for Processed Events Topic
# Opens a console consumer CLI to inspect transformed output messages from the beginning of the topic
docker exec -it kafka-1 /opt/kafka/bin/kafka-console-consumer.sh \
  --topic processed-user-events \
  --bootstrap-server kafka-1:29092 \
  --from-beginning
