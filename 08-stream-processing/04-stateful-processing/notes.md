# Source topic for Stateful Windowing Operations
# Raw click events (keyed by user_id) are consumed from this topic to aggregate count per window
docker exec -it kafka-1 /opt/kafka/bin/kafka-topics.sh --create \
  --topic user-clicks \
  --partitions 1 \
  --bootstrap-server kafka-1:29092

# Output topic for Stateful Windowing Operations
# Contains aggregated click counts per user emitted every 10-second tumbling window
docker exec -it kafka-1 /opt/kafka/bin/kafka-topics.sh --create \
  --topic clicks-per-window \
  --partitions 1 \
  --bootstrap-server kafka-1:29092

# Output Consumer: Consumes and displays aggregated window results in real-time
# Reads processed metrics (user click counts per 10s window) from 'clicks-per-window' topic from the beginning
docker exec -it kafka-1 /opt/kafka/bin/kafka-console-consumer.sh \
  --topic clicks-per-window \
  --from-beginning \
  --bootstrap-server kafka-1:29092
