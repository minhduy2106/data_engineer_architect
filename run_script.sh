docker exec -it data_engineer_architect-spark-master-1 spark-submit \
    --master spark://spark-master:7077 \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
    --conf spark.kafka.security.protocol=PLAINTEXT \
    /opt/bitnami/spark/jobs/spark_processor.py

