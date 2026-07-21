# Create topic for product table changelog
docker exec -it kafka-1 /opt/kafka/bin/kafka-topics.sh --create --if-not-exists \
  --topic product-updates \
  --partitions 1 \
  --replication-factor 1 \
  --bootstrap-server kafka-1:29092

# Create topic for order stream
docker exec -it kafka-1 /opt/kafka/bin/kafka-topics.sh --create --if-not-exists \
  --topic simple-orders \
  --partitions 1 \
  --replication-factor 1 \
  --bootstrap-server kafka-1:29092

# Create output topic for enriched orders
docker exec -it kafka-1 /opt/kafka/bin/kafka-topics.sh --create --if-not-exists \
  --topic enriched-orders \
  --partitions 1 \
  --replication-factor 1 \
  --bootstrap-server kafka-1:29092

# Output topic for Stream-Table Join (KStream-KTable Join)
# Contains enriched orders resulting from joining 'simple-orders' stream with in-memory product table
# Output Consumer: Consumes and displays enriched orders in real-time
# Reads JSON enriched results (order_id, product_id, product_name) from 'enriched-orders' topic from the beginning
docker exec -it kafka-1 /opt/kafka/bin/kafka-console-consumer.sh \
  --topic enriched-orders \
  --bootstrap-server kafka-1:29092 \
  --from-beginning

# Source topic for Stream-Table Join (acts as KTable changelog)
# Product catalog events (keyed by product_id, value=product_name) are consumed to build and continuously update in-memory product table
# Input Producer: Produces product catalog updates for table building
# Sends key-value records (format product_id:product_name) to 'product-updates' topic for product-table-builder group
docker exec -it kafka-1 /opt/kafka/bin/kafka-console-producer.sh \
  --topic product-updates \
  --property "parse.key=true" \
  --property "key.separator=:" \
  --bootstrap-server kafka-1:29092

# Source topic for Stream-Table Join (acts as KStream)
# Raw order events (keyed by order_id, value=product_id) are consumed to be enriched with product data
# Input Producer: Produces raw orders for enrichment processing
# Sends key-value records (format order_id:product_id) to 'simple-orders' topic for order-enrichment-group
docker exec -it kafka-1 /opt/kafka/bin/kafka-console-producer.sh \
  --topic simple-orders \
  --property "parse.key=true" \
  --property "key.separator=:" \
  --bootstrap-server kafka-1:29092
