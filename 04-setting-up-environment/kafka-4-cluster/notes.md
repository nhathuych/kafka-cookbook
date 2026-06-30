# Start a temporary Kafka container and open an interactive Bash shell
docker run -it --rm apache/kafka:4.0.0 bash

# Generate a new Cluster ID (UUID) for a Kafka KRaft cluster
docker run --rm apache/kafka:4.0.0 /opt/kafka/bin/kafka-storage.sh random-uuid

# Initialize the Kafka KRaft storage with the specified Cluster ID (run once before the first startup)
docker run --rm -v $(pwd)/kafka-data:/tmp/kraft-data apache/kafka:4.0.0 /opt/kafka/bin/kafka-storage.sh format --cluster-id iwVXtzdlQmmeiHnET1tkjA --config /opt/kafka/config/server.properties --standalone

# Create a new topic on the running Kafka broker
docker exec -it kafka-broker /opt/kafka/bin/kafka-topics.sh --create --topic my-first-topic --partitions 1 --replication-factor 1 --bootstrap-server kafka:9092
