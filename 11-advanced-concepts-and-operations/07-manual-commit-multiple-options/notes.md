# Reset consumer group to earliest to reprocess all messages
docker exec -it kafka-1 /opt/kafka/bin/kafka-consumer-groups.sh \
  --group demo-consumer-group \
  --topic go-app-events \
  --reset-offsets --to-earliest --execute \
  --bootstrap-server kafka-1:29092
