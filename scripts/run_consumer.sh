docker exec -it kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic loanPredictions \
  --from-beginning