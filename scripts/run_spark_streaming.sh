docker exec -i spark_streaming bash -c "
  source /opt/conda/etc/profile.d/conda.sh && \
  conda activate py37 && \
  spark-submit \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.1 \
    --master spark://spark-master:7077 \
    --deploy-mode client \
    work/spark/streaming_job.py
"