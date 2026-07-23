# Create topic for distributed file ingestion
# Source topic for Distributed File Source Connector Pattern
# Stores file content streamed by Kafka Connect distributed workers
docker exec -it kafka-1 /opt/kafka/bin/kafka-topics.sh \
  --create \
  --topic distributed-file-topic \
  --partitions 1 \
  --bootstrap-server kafka-1:29092

# Navigate to Kafka Connect configuration directory
# Contains File Source connector JSON configs
cd 10-kafka-connect

# Register File Source Connector via Kafka Connect REST API (Distributed Mode)
# Creates and starts FileStreamSourceConnector from file-source-config.json
# Ingests data from local file into distributed-file-topic
curl -X POST -H "Content-Type: application/json" --data '@file-source-config.json' http://localhost:8083/connectors

# Register multiple File Source Connectors for distributed ingestion demo
# Each config defines a separate FileStreamSource task for scaling demonstration
curl -X POST -H "Content-Type: application/json" --data '@file-source-config1.json' http://localhost:8083/connectors
curl -X POST -H "Content-Type: application/json" --data '@file-source-config2.json' http://localhost:8083/connectors
curl -X POST -H "Content-Type: application/json" --data '@file-source-config3.json' http://localhost:8083/connectors

# List all connectors
curl http://localhost:8083/connectors/
# Get status of distributed-file-source connector
curl http://localhost:8083/connectors/distributed-file-source/status
# Pause distributed-file-source connector
curl -X PUT http://localhost:8083/connectors/distributed-file-source/pause
# Resume distributed-file-source connector
curl -X PUT http://localhost:8083/connectors/distributed-file-source/resume
# Delete distributed-file-source connector
curl -X DELETE http://localhost:8083/connectors/distributed-file-source/

# Output topic for Distributed File Source Pattern
# Contains file lines streamed into distributed-file-topic by Connect workers
# Output Consumer: Consumes and displays file data in real-time for verification
# Reads records from distributed-file-topic from the beginning
docker exec -it kafka-1 /opt/kafka/bin/kafka-console-consumer.sh \
  --topic distributed-file-topic \
  --from-beginning \
  --bootstrap-server kafka-1:29092
