docker exec kafka /opt/kafka/bin/kafka-topics.sh --create \
  --topic rtsp-source-commands \
  --bootstrap-server localhost:9092 \
  --partitions 1 \
  --replication-factor 1 \
  --config cleanup.policy=compact \
  --config min.compaction.lag.ms=60000 \
  --config segment.ms=3600000 \
  --config retention.ms=604800000   # 7 days fallback