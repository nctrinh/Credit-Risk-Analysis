TOPICS=("loanEvents" "loanPredictions")

for t in "${TOPICS[@]}"; do
  docker exec -i kafka kafka-topics --create \
    --bootstrap-server localhost:9092 \
    --topic $t \
    --partitions 3 \
    --replication-factor 1 \
    --if-not-exists
done

docker exec -i kafka kafka-topics --list --bootstrap-server localhost:9092
